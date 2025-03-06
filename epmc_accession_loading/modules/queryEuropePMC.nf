process QUERY_EUROPEPMC {
    label 'process_tiny'
    debug true

    input:
    path(cursors)
    path(accession_types)
    val page_size
    val limit

    output:
    path("epmc_jsons/**.json"), emit: epmc_jsons
    path("cursors.txt"), emit: cursors

    script:
    """
    query_epmc.py --cursor-file ${cursors} --accession-types ${accession_types} --outdir epmc_jsons/ \
    --page-size ${page_size} --limit ${limit}
    """
}