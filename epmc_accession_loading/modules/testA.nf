process TESTA {
    label 'process_single'
    debug true

    input:
        val meta

    output:
        // tuple val(meta), path(result_json), stdout // emit: next_cursor
        tuple val(meta), path(result_json), path(next_cursor)
        // val(meta)
        // path(result_json)
        // path(next_cursor)
        // stdout emit: next_cursor

    script:
        result_json = "out.json"
        next_cursor = "next_cursor.txt"
        """
        testA.py '${meta.cursor}'
        """
        // next_cursor = file($cursor_out).text
}
