# Use the official Python image from the Docker Hub
FROM python:3.12

# Create the working directory
WORKDIR /code

# Copy the contents of the src/api directory into the container
COPY ./src/pipeline /code/src/pipeline

# Install the required packages
RUN pip install --no-cache-dir --upgrade -r /code/src/pipeline/requirements.txt

CMD ["python", "/code/pipeline/main.py"]
