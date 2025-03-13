process QUERY_EUROPEPMC {
    label 'process_tiny'
    debug true

    input:
    path(accession_types)
    val page_size
    val limit
    val db
    val db_user
    val db_pass

    output:
    path("epmc_jsons/**.json"), emit: epmc_jsons
    path("cursors.txt"), emit: cursors

    script:
    """
    query_epmc.py --accession-types ${accession_types} --outdir epmc_jsons/ \
    --page-size ${page_size} --limit ${limit} \
    --db ${db} --sqluser ${db_user} --sqlpass ${db_pass}
    """
}