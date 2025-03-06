process WRITE_TO_GBC {
    label 'process_tiny'
    debug true

    input:
    tuple path(json_file), path(accession_types), val(db)

    output:
    path(summary_file)

    script:
    summary_file = "${json_file.baseName}.summary.txt"
    """
    load_to_gbc.py --json ${json_file} --accession-types ${accession_types} --db ${db} \
    --summary ${summary_file}
    """
}