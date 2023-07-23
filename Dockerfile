# Use the jupyter/pyspark-notebook base image
FROM jupyter/pyspark-notebook

# Set the Python version to be used by PySpark
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

# Set the working directory to /opr/application inside the container
WORKDIR /opr/application

# Copy the requirements.txt file from the local directory to the container's working directory
COPY requirements.txt .

# Install the Python dependencies specified in the requirements.txt file
RUN pip install -r requirements.txt

# Copy the 'resources/' folder from the local directory to the container's working directory
COPY resources/ resources

# Copy the main.py file from the local directory to the container's working directory
COPY main.py ./main.py

# Copy the config.py file from the local directory to the container's working directory
COPY config.py ./config.py

# Set the command to run when the container starts
CMD python -u main.py
