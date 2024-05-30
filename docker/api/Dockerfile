# Use the official Python image from the Docker Hub
FROM python:3.12

# Create the working directory
WORKDIR /code

# Copy the contents of the src/api directory into the container
COPY ./src/api /code/src/api

# Install the required packages
RUN pip install --no-cache-dir --upgrade -r /code/src/api/requirements.txt

EXPOSE 80

CMD ["fastapi", "dev", "./api/main.py", "--host", "0.0.0.0","--port", "80"]
#CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "80"]

