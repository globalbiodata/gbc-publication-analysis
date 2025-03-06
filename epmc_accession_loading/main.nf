nextflow.enable.dsl=2

// Run the workflow
include { QUERY_EUROPEPMC } from './modules/queryEuropePMC'
include { WRITE_TO_GBC } from './modules/writeToGBC'

workflow {
    main:
        if (!file('cursors.txt').exists()) {
            file('cursors.txt').text = ''
        }

        query = QUERY_EUROPEPMC(
            file('cursors.txt'),
            file(params.accession_types),
            params.page_size,
            params.limit
        )
        query.epmc_jsons | flatten
        | view

        query.epmc_jsons
        | flatten
        | map { json -> [json, file(params.accession_types), params.db] }
        | WRITE_TO_GBC

        // query.epmc_json_dir.subscribe { println "Query results are in ${it}" }

        // query.epmc_json_dir.eachFileRecurse(groovy.io.FileType.FILES) { file ->
        //     println "${file.getName()} - size: ${file.size()}"
        // }

        // Channel.fromPath(query.epmc_json_dir + '/**.json')
        // | map { file -> tuple(file, params.accession_types, params.db) }
        // | WRITE_TO_GBC
}
