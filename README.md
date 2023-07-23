**July 2023** 

**Welcome to the C2i Genomics Data Engineer Assignment!** 

Your task is to build a data pipeline that combines data from three input CSV files and outputs a Parquet file. The objective is to perform data integration by joining the information from these files, ensuring data cleanliness by eliminating duplicates, and encapsulating the entire solution within a Docker container for seamless deployment. 

In  this  assignment,  you  will  demonstrate  your  skills  in  Python  programming,  data  processing,  and Dockerization.   

**The input files are as follows:** 

1. CSV 1 - Projects, Studies, and Cohorts: 
   1. This file contains information about projects, studies, and cohorts. 
   1. Each project is managed by a project manager. 
   1. Projects can have one or more studies. 
   1. Studies can have one or more cohorts. 
   1. The combination of Project code, Study code, and Cohort code is unique. 
1. CSV 2 - Subjects and Samples: 
   1. This file contains information about subjects and their associated samples. 
   1. Each subject belongs to a specific cohort (one subject cannot be part of two cohorts). 
   1. Subjects can have one or more samples. 
1. CSV 3 - Sample Processing Results: 
- This file contains the results of processing samples through the c2i processing pipeline. 
- Each sample is associated with a subject. 
- The fields in this file include: 
- cancer\_detected\_(yes\_no\_na): Indicates whether cancer was detected in the sample. 
- detection\_value: Represents the amount of cancer detected (ranging from 0 to 1). 
- sample\_quality: Represents the quality of the data (ranging from 0 to 1). 
- sample\_quality\_minimum\_threshold: Specifies the threshold for declaring a sample as low-quality. 
- sample\_status: Indicates the status of the sample (running/finished/failed). 
- fail\_reason: Specifies the reason for sample failure (technical/quality). 
- date of run: Represents the date the sample was processed. 

**Your task is to:** 

1. Write a Python script that reads the three input CSV files. 
1. Join the data from these files based on the specified relationships (e.g., Project code, Study code, Cohort code). 
1. Perform data cleaning and remove any duplicate records. 
1. Save the cleaned and transformed data into a single Parquet file. 
1. Create a csv summary file per project, study, cohort with the following information: 
   1. How many samples were detected 
   1. Percentage of finished samples according to sample\_status 
   1. Lowest detection value for detected samples 
1. Dockerize your Python script, ensuring that all necessary dependencies are included in the Docker container. 
1. Provide clear instructions in a README file on how to build, run, and test the Docker container. 

**Deliverables:** 

1. Python script: 
   1. data\_pipeline.py : Python script that performs data extraction, transformation, cleaning, and saving the joined data as a Parquet file. 
1. Docker: 
   1. Dockerfile : Dockerfile for building the data pipeline container. 
1. Documentation: 
   1. README.md : Instructions on how to build, run, and test the data pipeline using Docker. 
1. Results: 
- output.parquet : Merged results output file in a parquet format. 
- summary.csv : Summary per project, study, cohort 

Feel free to contact us with any  questions and/or needed clarifications. 

Please send the deliverables in a zip file to[ alina.aven@c2i-genomics.com ](mailto:alina.aven@c2i-genomics.com)and zohar@c2i-genomics.com  Good luck! 

Thank you for your time,  

C2i Genomics Data Engineering 
