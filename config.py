# Configuration settings for the data processing script

# Path to the CSV file containing information about projects, studies, and cohorts.
config = {
    'projects_studies_cohorts': 'resources/excel_1_project_study_cohort.csv',

    # Path to the CSV file containing information about subjects and their associated samples.
    'subjects_and_samples': 'resources/excel_2_subject_samples.csv',

    # Path to the CSV file containing sample processing results.
    'sample_processing_results': 'resources/excel_3_sample_run_results.csv',

    # Folder where the output files will be stored.
    'output_folder': 'output',

    # Folder where the consolidated data will be stored by project.
    'data_folder': 'sample_results_by_project',

    # Log level for the script. It controls the verbosity of log messages.
    'log_level': 'info',

    # Folder where project summary files will be stored.
    'project_summary_folder': 'project_summary'
}
