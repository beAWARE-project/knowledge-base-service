# Use an official Python runtime as a parent image
FROM python:3.7-slim

COPY src/ /usr/src/knowledge-base-service/

COPY requirements.txt /usr/src/knowledge-base-service/

# Set the working directory to /usr/src/knowledge-base-service/
WORKDIR /usr/src/knowledge-base-service/

# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt
RUN rm requirements.txt

# Run app.py when the container launches
CMD ["python3", "kb_service.py"]