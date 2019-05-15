# Use an official Python runtime as a parent image
FROM python:3.7-slim

# install git 
RUN apt-get update && apt-get install -y git

# download the project from github (select the correct branch (master/Version2))
RUN git clone --single-branch --branch Version2 https://github.com/beAWARE-project/knowledge-base-service.git /kbs_workdir

# Copy the current directory contents into the container at /kbs_workdir
COPY . /kbs_workdir

# Set the working directory to /kbs_workdir
WORKDIR /kbs_workdir

# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Set the working directory to /kbs_workdir/src
WORKDIR /kbs_workdir/src

#clear sqlite
RUN python3 clear_KB_v3.py

# Run app.py when the container launches
CMD ["python3", "kb_service.py"]
