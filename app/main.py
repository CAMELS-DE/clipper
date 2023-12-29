# dictionary, mapping the variable path prefixes to the variable name(s)
# in the HYRAS netCDF files
VARIABLES = {
    'Humidity': 'hurs', 
    'Precipitation': '', 
    'RadiationGlobal': '', 
    'TemperatureMax': '', 
    'TemperatureMean': '', 
    'TemperatureMin': ''
}

# instantiate a Processor object. That is populated with settings from an .env file, 
# the envrionment variables, holds some defaults or can be overwritten by kwargs.
# The Processor object is used to access the cloud storage buckets, and track the 
# currently processed, finished or errored files, to orchestrate the processing
# between different containers, either in the google cloud or locally.
# processor = Processor()


