Improved Documentation File for C2i Genomics Assignment:

# C2i Genomics Assignment Documentation

## Summary

This project implements a data pipeline that combines information from three input CSV files and produces Parquet files after cleaning duplicate rows. The main objective is to perform data integration by joining relevant information from these files. The project also includes a Dockerfile to containerize the solution.

## main.py

### ETL function:

#### Extract:
The ETL (Extract, Transform, Load) process begins by reading each of the three input CSV files and promptly cleaning them from duplicate records. The `drop_duplicates` function is utilized for this purpose, taking into account the fields specified in the assignment instructions. By cleaning the data as soon as it is extracted, we improve performance by reducing the data size we work with.

#### Transform:
The transformation step involves joining the data from the three files based on specified relationships, such as Project code, Study code, and Cohort code. The joins are performed in two stages, starting with left joins and then right joins to retain only the relevant data.

#### Load:
After the data has been transformed, the resulting data frame is saved as a single Parquet file.

### clean_columns_name function:
The column names provided by the CSV file headers were inconvenient as they contained brackets, spaces, and other undesirable characters. To address this, a `clean_columns_name` function is implemented to rename the row titles. In more substantial projects, it is recommended to extract this utility function into a separate utils class for better organization.

### build_project_summary:
This function generates a CSV summary file for each project, study, and cohort. The summary includes the following information:
- Number of samples detected
- Percentage of finished samples based on the sample_status
- Lowest detection value among the detected samples

## Dockerize:

To build a Docker image of the application, run the following command:
```
docker build -t c2igenomics .
```

To create a container without volume, execute the following command:
```
docker run c2igenomics:latest
```

To create a container with volume, add your local path and execute the following command:
```
docker run -v /absolute/local/path/:/opr/application/output c2igenomics:latest
```
## Conclusion

This documentation provides an overview of the C2i Genomics Assignment project. It outlines the main functionality of the `main.py` script, including the ETL process, data transformation, and loading of results. Additionally, it highlights the use of the `clean_columns_name` function for improving column names and the `build_project_summary` function to create informative CSV summary files. Lastly, instructions for Dockerizing the application are provided to facilitate easy deployment.
