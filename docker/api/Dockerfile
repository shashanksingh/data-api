# Use the official Python image from the Docker Hub
FROM python:3.12

# Create the working directory
WORKDIR /code

# Copy the contents of the src/api directory into the container
COPY ./src/api /code/src/api

# Install the required packages
RUN pip install --no-cache-dir --upgrade -r /code/src/api/requirements.txt

CMD ["fastapi", "run", "./api/main.py", "--port", "6000"]

