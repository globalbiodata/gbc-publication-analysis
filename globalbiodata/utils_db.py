from __future__ import annotations

import sys
import random
import time

import sqlalchemy as db
from sqlalchemy.dialects.mysql import insert # for on_duplicate_key_update
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import OperationalError

from typing import Optional

# ---------------------------------------------------------------------------- #
# Database helper methods                                                      #
# ---------------------------------------------------------------------------- #

# manually defined primary and unique keys for tables
table_keys = {
    'resource': {'pk': ['id'], 'uk': ['short_name', 'url_id', 'version_id']},
    'url': {'pk': ['id'], 'uk': ['url']},
    'connection_status': {'pk': ['url_id', 'date'], 'uk': []},
    'version': {'pk': ['id'], 'uk': ['name', 'date']},
    'publication': {'pk': ['id'], 'uk': ['pubmed_id', 'pmc_id']},
    'grant': {'pk': ['id'], 'uk': ['ext_grant_id', 'grant_agency_id']},
    'grant_agency': {'pk': ['id'], 'uk': ['name_hash']},
    'resource_publication': {'pk': ['resource_id', 'publication_id'], 'uk': []},
    'resource_grant': {'pk': ['resource_id', 'grant_id'], 'uk': []},
    'publication_grant': {'pk': ['publication_id', 'grant_id'], 'uk': []},
    'accession': {'pk': ['accession'], 'uk': []},
    'accession_publication': {'pk': ['accession', 'publication_id'], 'uk': []},
    'resource_mention': {'pk': ['publication_id', 'resource_id', 'matched_alias'], 'uk': []},
}

def _get_primary_keys(table, conn):
    cached_pks = table_keys.get(table.name, {}).get('pk', None)
    if cached_pks is not None:
        return cached_pks
    return db.inspect(conn).get_pk_constraint(table.name)['constrained_columns']

def _get_unique_keys(table, conn):
    cached_uks = table_keys.get(table.name, {}).get('uk', None)
    if cached_uks is not None:
        return cached_uks
    insp_uks = db.inspect(conn).get_unique_constraints(table.name)
    try:
        return insp_uks[0]["column_names"]
    except IndexError:
        return []

def _get_all_keys(table, conn):
    return _get_primary_keys(table, conn) + _get_unique_keys(table, conn)

def _fetch_id_from_unique_keys(table, data, conn, debug=False):
    # detect unique keys for table and use to query id
    uniq_col_names = _get_unique_keys(table, conn)
    if not uniq_col_names:
        return 0

    wheres = [table.columns.get(ucn) == data[ucn] for ucn in uniq_col_names]
    if debug:
        print(f"--> finding ids with cols: {', '.join(uniq_col_names)}")

    select = db.select(table.c.id).where(db.and_(*wheres))
    result = conn.execute(select).fetchone()
    if result is None:
        raise ValueError(f"Entity not found in table {table.name} with unique keys: ", {k:v for k, v in data.items() if k in uniq_col_names})
    return result[0]

def _remove_key_fields(table, conn, data): # also remove empty values
    key_names = _get_all_keys(table, conn)
    return {k:v for k, v in data.items() if (k not in key_names and v is not None)}

def _stringify_data(data):
    for k, v in data.items():
        if type(v) is list:
            data[k] = '; '.join(v)
    return data

def _get_db_name(engine):
    db_name = engine.url.database
    if db_name is None:
        # grab from the raw connection
        conn = engine.connect()
        raw_conn = conn.connection.connection
        db_name = raw_conn.db.decode()

    return db_name

_max_lens = {}
def _get_max_len(col_name, table_name, conn=None, engine=None):
    """Return CHARACTER_MAXIMUM_LENGTH for a column using an existing connection if provided.
    Falls back to engine.connect() only when necessary.
    """
    global _max_lens
    col_key = f"{table_name}.{col_name}"
    if col_key not in _max_lens:
        close_after = False
        if conn is None:
            if engine is None:
                raise ValueError("_get_max_len requires either an engine or an open connection")
            conn = engine.connect()
            close_after = True

        # Determine current database name (schema) in a driver-agnostic way
        try:
            db_name = conn.execute(db.text("SELECT DATABASE()")).scalar()
        except Exception:
            # Fallback: try the engine helper if available
            if engine is not None:
                db_name = _get_db_name(engine)
            else:
                raise

        max_len_sql = db.text(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table AND COLUMN_NAME = :col
            """
        )
        rows = conn.execute(max_len_sql, {"db": db_name, "table": table_name, "col": col_name}).fetchall()
        for result in rows:
            _max_lens[col_key] = result[0]

        if close_after:
            conn.close()

    return _max_lens.get(col_key, None)

def insert_into_table(
    table_name: str,
    data: dict,
    conn: Optional[Connection] = None,
    engine: Optional[Engine] = None,
    debug: bool = False,
    filter_long_data: bool = False,
    retry_on_deadlock: bool = True,
    max_retries: int = 5,
    base_delay: float = 0.2,
) -> int:
    """Insert-or-update a row and return its id.

    Args:
        table_name (str): Name of the table to insert into.
        data (dict): Dictionary of column names and values to insert.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.
        filter_long_data (bool, optional): If `True`, move overlength strings to long_text table.
        retry_on_deadlock (bool, optional): If `True`, retry on deadlock errors.
        max_retries (int, optional): Maximum number of retries on deadlock.
        base_delay (float, optional): Base delay in seconds for exponential backoff.

    Returns:
		The ID/PK of the inserted or updated row.
    """
    metadata_obj = db.MetaData()

    conn_created = False
    if conn is None:
        if engine is None:
            raise ValueError("insert_into_table requires either an engine or an open connection")
        conn = engine.connect()
        conn_created = True

    # Reflect the table using the ENGINE (separate pooled connection) to avoid
    # starting an implicit transaction on our working connection.
    table = db.Table(table_name, metadata_obj, autoload_with=conn)
    pk_cols = _get_primary_keys(table, conn)
    data = _stringify_data(data)

    if debug:
        print(f"\n--> Inserting into table: {table_name}")
        print(data)
        print(f"Columns: {', '.join(table.columns.keys())}")
        print(f"Primary keys: {', '.join(pk_cols)}")

    # Optionally move overlength strings to long_text and replace with reference token
    if filter_long_data:
        for k, v in list(data.items()):
            this_max_len = _get_max_len(k, table_name, conn=conn, engine=engine)
            if v and this_max_len and isinstance(v, str) and len(v) > this_max_len:
                longtext_id = insert_into_table('long_text', {'text': v}, conn=conn, engine=engine, debug=debug)
                data[k] = f"long_text({longtext_id})"

    # We "own" the transaction if we opened this connection in this function.
    # Reflection above may have triggered an implicit txn elsewhere; don't let that
    # disable retries. We manage our own explicit txn when we created the conn.
    own_txn = conn_created
    # print(f"conn_created={conn_created}, own_txn={own_txn}, conn.in_transaction()={conn.in_transaction()}")

    # Retry loop only if we control our own transaction
    attempts = 0
    while True:
        attempts += 1
        # If we own the connection, start an explicit transaction only if one isn't already active
        implicit_txn = conn.in_transaction()
        trans = conn.begin() if (own_txn and not implicit_txn) else None
        try:
            insert_stmt = insert(table).values(data)
            data_no_pks = _remove_key_fields(table, conn, data)

            if data_no_pks:  # typical path: upsert (ON DUPLICATE KEY UPDATE)
                if pk_cols and len(pk_cols) == 1:
                    # single primary key: use LAST_INSERT_ID() hack to get the id of existing row if no insert occurred
                    pk_name = pk_cols[0]
                    data_no_pks[pk_name] = db.func.last_insert_id(table.c[pk_name])

                do_update_stmt = insert_stmt.on_duplicate_key_update(**data_no_pks)
                if debug:
                    print(f"Updating {table_name} with data: {data_no_pks}")
                result = conn.execute(do_update_stmt)
            else:  # tables that are pure key rows
                result = conn.execute(insert_stmt.prefix_with('IGNORE'))

            if trans is not None:
                trans.commit()
            elif own_txn and implicit_txn and conn.in_transaction():
                # We own the connection and were already in an implicit txn (likely started by prior metadata queries)
                conn.commit()

            inserted_pk = result.inserted_primary_key[0] if result.inserted_primary_key else None
            affected_rows = result.rowcount

            # Determine the resulting id to return
            if (not inserted_pk) or (inserted_pk and affected_rows == 0):
                # entity existed and was not updated
                existing_id = conn.execute(db.text("SELECT LAST_INSERT_ID()")).scalar() if len(pk_cols) == 1 else _fetch_id_from_unique_keys(table, data, conn, debug=debug)
                if debug:
                    print(f"Entity already exists. Fetched id: {existing_id}")
                this_id = existing_id
            elif (inserted_pk and affected_rows > 1):
                # entity existed and was updated (MySQL reports >1 affected rows on upsert-update)
                this_id = inserted_pk
                if debug:
                    print(f"Entity already exists. Updated id: {this_id}")
            else:
                this_id = inserted_pk
                if debug:
                    print(f"New entity added. Inserted id: {this_id}")

            # Success; break retry loop
            break

        except OperationalError as e:
            # MySQL deadlock / lock wait timeout codes
            code = None
            try:
                code = e.orig.args[0]
            except Exception:
                pass

            if trans is not None:
                try:
                    trans.rollback()
                except Exception:
                    pass
            elif own_txn and conn.in_transaction():
                try:
                    conn.rollback()
                except Exception:
                    pass

            # If we don't own the transaction or retries disabled, re-raise immediately
            if not own_txn or not retry_on_deadlock or code not in (1213, 1205):
                raise

            # Backoff and retry (we own the txn)
            if attempts > max_retries:
                raise
            # Exponential backoff with jitter
            delay = (base_delay * (2 ** (attempts - 1))) * (1 + 0.25 * random.random())
            if debug:
                sys.stderr.write(f"[retry] {table_name}: OperationalError {code}; attempt {attempts}/{max_retries}; sleeping {delay:.2f}s\n")
            time.sleep(delay)
            continue

        except Exception as e:
            if trans is not None:
                try:
                    trans.rollback()
                except Exception:
                    pass
            elif own_txn and conn.in_transaction():
                try:
                    conn.rollback()
                except Exception:
                    pass
            sys.stderr.write(f"Transaction rolled back due to: {e}\n")
            raise
        finally:
            if conn_created and own_txn and not conn.closed:
                # keep connection open for caller if they provided it; otherwise close at end
                pass

    if conn_created:
        conn.close()

    return this_id

def delete_from_table(
    table_name: str,
    data: dict,
    conn: Optional[Connection] = None,
    engine: Optional[Engine] = None,
    debug: bool = False
) -> int:
    """Delete rows from a table matching the provided data.

    Args:
        table_name (str): Name of the table to delete from.
        data (dict): Dictionary of column names and values to match for deletion.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		Number of rows deleted.
    """
    metadata_obj = db.MetaData()

    conn_created = False
    if conn is None:
        if engine is None:
            raise ValueError("select_from_table requires either an engine or an open connection")
        conn = engine.connect()
        conn_created = True

    # Reflect the table using the active connection
    table = db.Table(table_name, metadata_obj, autoload_with=conn)
    data = _stringify_data(data)

    if debug:
        print(f"\n--> Deleting from table: {table_name} WHERE:")
        print(' AND '.join([f"{k} == {data[k]}" for k in data.keys()]))

    trans = conn.begin() if not conn.in_transaction() else None  # Begin a transaction if we're not already in one
    try:
        wheres = [table.columns.get(c) == data[c] for c in data.keys()]
        del_result = conn.execute(db.delete(table).where(db.and_(*wheres)))
        if debug:
            print(f"Deleted {del_result.rowcount} rows.")
        trans.commit()  # Commit the transaction
    except Exception as e:
        if trans:
            trans.rollback()  # Rollback the transaction if an error occurs
            sys.stderr.write(f"Transaction rolled back due to: {e}\n")
        raise
    finally:
        if conn_created:
            conn.close()  # Close the connection

    return del_result.rowcount

def select_from_table(
    table_name: str,
    data: dict = {},
    join_table: str = None,
    order_by: list = None,
    conn: Optional[Connection] = None,
    engine: Optional[Engine] = None,
    debug: bool = False
) -> list:
    """Select rows from a table matching the provided data.

    Args:
        table_name (str): Name of the table to select from.
        data (dict, optional): Dictionary of column names and values to match for selection.
        join_table (str, optional): Name of a table to join with.
        order_by (list, optional): Column name(s) to order the results by.
        conn (Optional[Connection], optional): SQLAlchemy Connection object.
        engine (Optional[Engine], optional): SQLAlchemy Engine object.
        debug (bool, optional): If `True`, print debug information.

    Returns:
		List of dictionaries representing the selected rows.
    """
    metadata_obj = db.MetaData()

    # Ensure we have a live connection before reflecting
    conn_created = False
    if conn is None:
        if engine is None:
            raise ValueError("select_from_table requires either an engine or an open connection")
        conn = engine.connect()
        conn_created = True

    # Reflect using the active connection (works in SA 1.4/2.0)
    table = db.Table(table_name, metadata_obj, autoload_with=conn)
    if join_table:
        join_tbl = db.Table(join_table, metadata_obj, autoload_with=conn)
        table = table.join(join_tbl)
        # print("JOINED TABLE COLUMNS:", table.columns.keys())

    if debug:
        print(f"\n--> Selecting from table: {table_name} WHERE:")
        print('AND '.join([f"{k} == '{data[k]}'" for k in data.keys()]))

    wheres = [
        table.columns.get(c).in_(data[c]) if isinstance(data[c], list)
        else table.columns.get(c) == data[c]
        for c in data
    ]

    # construct select statement with correct options
    stmt = db.select(table)
    if wheres:
        stmt = stmt.where(db.and_(*wheres))
    if order_by:
        if isinstance(order_by, list):
            order_cols = [table.columns.get(col) for col in order_by if table.columns.get(col) is not None]
            if order_cols:
                stmt = stmt.order_by(*order_cols)
        else:
            order_col = table.columns.get(order_by)
            if order_col is not None:
                stmt = stmt.order_by(order_col)
    result = conn.execute(stmt).fetchall()

    # convert result to list of dicts
    d_result = [dict(zip(table.columns.keys(), list(r))) for r in result]

    if conn_created:
        conn.close()
    return d_result