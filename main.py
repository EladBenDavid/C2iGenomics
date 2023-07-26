import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import min
from config import config  # Import configuration variables
import pyspark.sql.functions as F


def etl():
    # Extract, Transform, and Load (ETL) process

    # Start ETL process for projects, studies, and cohorts data
    print('start scrubbing')

    # Read projects_studies_cohorts data from CSV and remove duplicates
    projects_studies_cohorts_df = spark.read.csv(config['projects_studies_cohorts'], sep=',', inferSchema=True,
                                              header=True)
    projects_studies_cohorts_df = projects_studies_cohorts_df.drop_duplicates(
        ['project_code', 'study_code', 'study_cohort_code'])

    # Clean column names for projects_studies_cohorts DataFrame
    projects_studies_cohorts_df = clean_columns_name(projects_studies_cohorts_df)

    # Read subjects_and_samples data from CSV and remove duplicates
    subjects_and_samples_df = spark.read.csv(config['subjects_and_samples'], sep=',', inferSchema=True, header=True)
    subjects_and_samples_df = subjects_and_samples_df.drop_duplicates(subset=['subject_id', 'study_cohort_code'])

    # Clean column names for subjects_and_samples DataFrame
    subjects_and_samples_df = clean_columns_name(subjects_and_samples_df)

    # Read sample_processing_results data from CSV and remove duplicates
    sample_processing_results_df = spark.read.csv(config['sample_processing_results'], sep=',', inferSchema=True,
                                               header=True)
    sample_processing_results_df = sample_processing_results_df.drop_duplicates(subset=['sample_id'])

    # Clean column names for sample_processing_results DataFrame
    sample_processing_results_df = clean_columns_name(sample_processing_results_df)

    print('scrubbing done')

    # Start transformation process
    print('start transformation')

    # Join subjects_and_samples and sample_processing_results on 'sample_id'
    sample_results_by_subject_df = subjects_and_samples_df.join(sample_processing_results_df, ['sample_id'], 'left')

    # Join sample_results_by_subject with projects_studies_cohorts on 'project_code', 'study_code', 'study_cohort_code'
    samples_results_by_project_df = sample_results_by_subject_df.join(projects_studies_cohorts_df,
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
    samples_results_by_project_df.write.option('header', True) \
        .mode('overwrite') \
        .parquet(f"{config['output_folder']}/{config['data_folder']}")

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
    # Printing a message to indicate the start of the process
    print(f'start to build summaries')
    # List of columns to partition by
    partition_columns = ['project_code', 'study_code', 'study_cohort_code']
    # Reading data from Parquet files into a DataFrame using Spark
    df = spark.read.parquet(f"{config['output_folder']}/{config['data_folder']}/*.parquet")
    # Casting the 'detection_value' column to 'double' data type, selecting specific columns, and creating a new DataFrame
    df = df.withColumn("detection_value_double", df.detection_value.cast('double')).select(partition_columns + ["detection_value_double", 'sample_id', 'sample_status'])
    # Grouping the DataFrame by specified columns and calculating the number of distinct 'sample_id' per group
    samples_count_df = df.groupby(partition_columns).agg(countDistinct('sample_id').alias("samples_count"))
    # Filtering the DataFrame to exclude rows where 'sample_status' is "Finished", then grouping by specified columns,
    # and calculating the number of distinct 'sample_id' per group (representing the count of unfinished samples)
    finished_samples_count_df = df.filter((df['sample_status'] == "Finished")).groupby(partition_columns).\
        agg(countDistinct('sample_id').alias("finished_samples_count"))
    # Joining the two DataFrames 'samples_count_df' and 'finished_samples_count_df' based on the specified columns
    samples_count_join_finish_df = samples_count_df.join(finished_samples_count_df, partition_columns, 'left')
    # Calculating the percentage of finished samples for each group and selecting relevant columns
    finished_samples_rate_df = samples_count_join_finish_df.withColumn('finished_samples_rate', F.round((samples_count_join_finish_df['finished_samples_count'] * 100 / samples_count_join_finish_df['samples_count']), 2)).select(['finished_samples_rate', 'samples_count'] + partition_columns )
    # Filling any missing values in the 'finished_samples_rate' column with '0'
    fixed_finished_samples_rate_df = finished_samples_rate_df.fillna({'finished_samples_rate': '0'})
    # Calculating the minimum value of 'detection_value_double' for each group and joining with 'fixed_finished_samples_rate_df'
    min_value_df = df.groupby(partition_columns).agg(min('detection_value_double').alias('min_detection_value'))
    summery_df = fixed_finished_samples_rate_df.join(min_value_df, partition_columns, 'inner')
    # Writing the summary DataFrame to CSV files, partitioning by the specified columns
    summery_df.write.option("header", True).mode('overwrite').partitionBy(partition_columns). \
        csv(f"{config['output_folder']}/{config['project_summary_folder']}")
    # Printing a message to indicate that the summaries were written successfully
    print(f"summaries was written")



if __name__ == '__main__':
    # Main entry point of the script
    print('start c2igenomics')
    # Create a SparkSession
    spark = SparkSession.builder.master("local"). \
        appName("c2igenomics"). \
        getOrCreate()

    # Call the ETL process
    etl()

    # Build project summaries
    build_project_summary()

    print('c2igenomics done')

