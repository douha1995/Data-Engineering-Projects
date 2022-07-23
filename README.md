# Data-Enigineering-Projects
## First Project: Build data pipeline using generator 
### row by row process 
my objective is to process the parking violation 2018 dataset with the following steps:
https://www.kaggle.com/new-york-city/ny-parking-violations-issued?select=parking-violations-issued-fiscal-year-2018.csv
a- Keep only the violations issued by police (denoted by P in the data), to vehicles with the make FORD in NJ.
Replace P with police.
b- Concat house number, street name, and registration state fields into a single address field.
c- calculate the number of days between the violation issue date and the vehicle expiration date
d- Write the result’s in to a csv file with the header vehicle_make,issuing_agency,address.


### Mini batching
my objective is to create function which accumulate the rows in memory. When it hits a specified threshold(100000 row), then make the service call I simulate .
