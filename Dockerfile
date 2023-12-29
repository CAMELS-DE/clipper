ARG python_version=3.10.13
FROM ghcr.io/mmaelicke/zonal-variograms:v0.3.2

# Add the dependencies for this image
RUN pip install python-dotenv==1.0.0
RUN pip install pydantic==2.5.3
RUN pip install pydantic-settings==2.1.0
RUN pip install google-cloud-storage==2.14.0

# finally add the code
RUN mkdir /app
COPY ./app /app
WORKDIR /app

# set the default command
CMD ["python", "main.py"]