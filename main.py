import os
import re

from pyspark.sql import SparkSession
from config import config  # Import configuration variables


def etl():
    # Extract, Transform, and Load (ETL) process

    # Start ETL process for projects, studies, and cohorts data
    print('start scrubbing')

    # Read projects_studies_cohorts data from CSV and remove duplicates
    projects_studies_cohorts = spark.read.csv(config['projects_studies_cohorts'], sep=',', inferSchema=True,
                                              header=True)
    projects_studies_cohorts = projects_studies_cohorts.drop_duplicates(
        ['project_code', 'study_code', 'study_cohort_code'])

    # Clean column names for projects_studies_cohorts DataFrame
    projects_studies_cohorts = clean_columns_name(projects_studies_cohorts)

    # Read subjects_and_samples data from CSV and remove duplicates
    subjects_and_samples = spark.read.csv(config['subjects_and_samples'], sep=',', inferSchema=True, header=True)
    subjects_and_samples = subjects_and_samples.drop_duplicates(subset=['subject_id', 'study_cohort_code'])

    # Clean column names for subjects_and_samples DataFrame
    subjects_and_samples = clean_columns_name(subjects_and_samples)

    # Read sample_processing_results data from CSV and remove duplicates
    sample_processing_results = spark.read.csv(config['sample_processing_results'], sep=',', inferSchema=True,
                                               header=True)
    sample_processing_results = sample_processing_results.drop_duplicates(subset=['sample_id'])

    # Clean column names for sample_processing_results DataFrame
    sample_processing_results = clean_columns_name(sample_processing_results)

    print('scrubbing done')

    # Start transformation process
    print('start transformation')

    # Join subjects_and_samples and sample_processing_results on 'sample_id'
    sample_results_by_subject = subjects_and_samples.join(sample_processing_results, ['sample_id'], 'left')

    # Join sample_results_by_subject with projects_studies_cohorts on 'project_code', 'study_code', 'study_cohort_code'
    sample_results_by_project = sample_results_by_subject.join(projects_studies_cohorts,
                                                               ['project_code', 'study_code', 'study_cohort_code'],
                                                               'right')

    print('transformation done')

    # Start data load process
    print('start load')

    # Check if the output_folder exists, if not, create it
    if not os.path.exists(config['output_folder']):
        os.makedirs(config['output_folder'])
        print(f'{config["output_folder"]} folder was created')

    # Write sample_results_by_project DataFrame to parquet format
    sample_results_by_project.write.option('header', True) \
        .partitionBy('project_code') \
        .mode('overwrite') \
        .parquet(f"{config['output_folder']}/{config['consolidation_dat_folder']}")

    print(f'parquet file was written')


def clean_columns_name(df):
    # Clean column names in the DataFrame

    # Regular expression to remove brackets from column names
    remove_brackets_regex = re.compile('[\(\[].*?[\)\]]')

    # Get the old column names
    old_columns = df.columns

    # Initialize a new list to store the cleaned column names
    new_columns = []

    # Clean each column name and append it to the new_columns list
    for column in old_columns:
        column = remove_brackets_regex.sub('', column)  # Remove brackets from column name
        column = column.removesuffix(' ')  # Remove trailing spaces
        column = column.removeprefix(' ')  # Remove leading spaces
        column = column.removesuffix('_')  # Remove trailing underscores
        column = column.replace(' ', '_')  # Replace spaces with underscores
        new_columns.append(column)

    # Apply the new column names to the DataFrame
    new_df = df.toDF(*new_columns)

    print(f'rename columns {new_columns}')

    return new_df


def build_project_summary():
    # Build project summaries based on the consolidated data

    print(f'start to build project summary')

    # Get the list of partitions in the consolidation data folder
    partitions = os.listdir(f"{config['output_folder']}/{config['consolidation_dat_folder']}")

    # Define the header for the project summary DataFrame
    header = ["project_code", "samples_count", "finished_samples_rate", "min_detection_value"]

    # Iterate through each partition and build the project summary
    for partition in partitions:
        if 'project_code=' in partition:
            # Read the parquet files for each partition
            df = spark.read.parquet(
                f"{config['output_folder']}/{config['consolidation_dat_folder']}/{partition}/*.parquet")

            # Calculate samples count, finished samples count, and finished samples rate
            samples_count = df.select("sample_id").dropDuplicates().count()
            finished_samples_count = df.filter(~(df['sample_status'] != "Finished")). \
                select("sample_id").dropDuplicates().count()
            finished_samples_rate = int(finished_samples_count * 100 / samples_count)

            # Convert detection_value column to double and find the minimum value
            df = df.withColumn("detection_value_double", df.detection_value.cast('double'))
            min_detection_value = df.agg({"detection_value_double": "min"}).collect()[0][0]

            # Extract the project_code from the partition name
            project_code = partition.replace('project_code=', '')

            # Create a DataFrame with the project summary information
            result = [(project_code, samples_count, finished_samples_rate, min_detection_value)]
            result_df = spark.createDataFrame(result, header)
            result_df = result_df.repartition(1)

            # Write the project summary DataFrame to a CSV file
            result_df.write.option("header", True).mode('overwrite'). \
                csv(f"{config['project_summary_folder']}/{project_code}")

            print(f"project summary for project code {project_code} was written")


if __name__ == '__main__':
    # Main entry point of the script

    print('start c2igenomics')

    # Create a SparkSession
    spark = SparkSession.builder.master("local"). \
        appName("c2igenomics").getOrCreate()

    # Call the ETL process
    etl()

    # Build project summaries
    build_project_summary()

    print('c2igenomics done')
