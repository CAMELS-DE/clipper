ARG python_version=3.10.13
FROM python:${python_version}

# Add the dependencies for the tool specification
RUN pip install json2args==0.6.1

# Install GDAL which will be used by geopandas
RUN pip install --upgrade pip
RUN apt-get update && apt-get install -y gdal-bin libgdal-dev
RUN pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

# Add the dependencies for this image
RUN pip install python-dotenv==1.0.0
RUN pip install typing_extensions==4.9.0
RUN pip install pydantic==2.5.3
RUN pip install pydantic-settings==2.1.0
RUN pip install google-cloud-storage==2.14.0
RUN pip install geopandas==0.14.2
RUN pip install xarray==2023.6.0
RUN pip install cftime==1.6.3
RUN pip install geocube==0.4.2
RUN pip install papermill==2.5.0
RUN pip install nbconvert==6.5.4


# create the structure
RUN mkdir -p /src/app
RUN mkdir /in
RUN mkdir /out

# COPY the files
COPY ./in /in
COPY ./src /src
COPY ./merit_hydro_catchments.gpkg /src/merit_hydro_catchments.gpkg
WORKDIR /src

# set the default command
CMD ["python", "main.py"]