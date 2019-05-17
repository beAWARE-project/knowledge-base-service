# Use an official Python runtime as a parent image
FROM python:3.7-slim


# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt


COPY src/* /usr/src/knowledge-base-service/


# Set the working directory to /usr/src/knowledge-base-service/
WORKDIR /usr/src/knowledge-base-service/

#clear sqlite
RUN python3 clear_KB_v3.py

# Run app.py when the container launches
# CMD ["python3", "kb_service.py"]
