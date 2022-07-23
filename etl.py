import csv
from datetime import datetime
import time

input_file = "./data/parking-violations-issued-fiscal-year-2018.csv"
output_file_path = "./data/nj_ford_transportation_issued_pv_2018.csv"

#  calculate the number of days between the violation issue date and the vehicle expiration date.
def get_days_between_violation_expiration_dates(singleRow):
    violation_issue_date_str = singleRow[5]
    vichel_expired_date_float =  singleRow[6]
    #print(violation_issue_date_str)
    # turn violation issued data into date
    violation_issue_date = datetime.strptime(violation_issue_date_str[:10], "%Y-%m-%d")

    # handle date not match format exception
    try:
        vichel_expired_date = datetime.strptime(str(vichel_expired_date_float).split(".")[0],"%Y%m%d")
        date_diff = vichel_expired_date - violation_issue_date
        date_diff = date_diff.days
    except ValueError as ve :
        date_diff = -1
    return date_diff

# simulate an external service call
def call_service(rows):
    time.sleep(20)
    return rows

#deal with mini batch data for external service using generator 
def batched_service_transforms(rows,batch_size = 100000):
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield from call_service(batch)
            batch = []
        yield from call_service(batch)
  
# stream data from input fileds using generator
file_to_read = open(input_file,"r")
file_extractor_generator = csv.reader(file_to_read)

#check batch transforms work or not 
all_stream = batched_service_transforms(file_extractor_generator)
next(all_stream)
next(all_stream)
# keep only required filds
# 2 => registration state, 7 => vehicle make, 8 => issuing agency, 23 => house number, 24 => street name
data_with_col_needed = ([row[2], row[7], row[8], row[23], row[24],row[4],row[12]] for row in file_extractor_generator)

# keep only violations issued by police, to vehicles with the make FORD in NJ
value_consistent = filter(
    lambda val1: all([val1[0] == "NJ", val1[1] == "FORD", val1[2] == "P"]), data_with_col_needed
)
print(next(value_consistent))
# replace P with police
data_stream_transformed = ([val1[0], val1[1], "police", val1[3], val1[4], val1[5],val1[6]] for val1 in value_consistent)
next(data_stream_transformed)
# concat house number, street name, registration state into a single address field
final_data_transformed = ([val[1], val[2], ", ".join([val[3], val[4], val[1]]),val[5],val[6], get_days_between_violation_expiration_dates(val)] for val in data_stream_transformed)

# write data into csv file 
file_to_write = open(output_file_path,'w')
data_loader =csv.writer(file_to_write, delimiter= ",",quotechar='"',quoting=csv.QUOTE_MINIMAL)

header = ["vehicle_make", "issuing_agency", "address","Issue Date","vehicle expiration date","date difference"]

data_loader.writerow(header)
print(next(final_data_transformed))
# stream data into an output file 
data_loader.writerows(final_data_transformed)
print("data loaded successfully")