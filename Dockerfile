# this Dockerfile is essentially adapted from a few tutorials I found on their website
# as I have not had any serious Docker experience in the past.

# we need a base image for python
FROM python:3

# set a working directory
WORKDIR /app

#
COPY . /app

#specify a port to be exposed, in our case it's 8080.
EXPOSE 8080

# Install any needed packages specified in requirements.txt (adapted from Docker's Get Started Tutorial)
# we need this to download Flask
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# run the file for the REST servoce
CMD ["python", "-u","asg2.py"]

