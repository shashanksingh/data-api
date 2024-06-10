# Use the official Python image from the Docker Hub
FROM python:3.12

# Create the working directory
WORKDIR /code

# Copy the contents of the src/api directory into the container
COPY ./src/pipeline /code/src/pipeline

# Install the required packages
RUN pip install --no-cache-dir --upgrade -r /code/src/pipeline/requirements.txt

# Copy the run script into the container
COPY ./docker/pipeline /docker/pipeline

# Make the script executable
RUN chmod +x /docker/pipeline/run.sh

CMD ["/docker/pipeline/run.sh"]
