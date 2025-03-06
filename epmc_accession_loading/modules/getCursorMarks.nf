process GET_CURSOR_MARKS {
    label 'process_single'
    debug true

    input:
        val meta

    output:
        path cursors_file

    script:
        cursors_file = "all_cursors.page1000.txt"
        """
        getCursorMarks.py ${cursors_file}
        """
}
