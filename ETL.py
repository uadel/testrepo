import glob
import pandas as pd
from datetime import datetime


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe



def extract(sourcefile = "bank_market_cap_1.json"):
    # Write your code here
    # Set column names
    extracted_data = pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
    # read file
    for jsonfile in glob.glob(sourcefile):

        # Append to data
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)

    return extracted_data

# Test
#print(extract_data)

# Write your code here
def exchange(ratefile = "exchange_rates.csv", currency = "GBP"):

    # initialise
    exchange_rate = 0

    # read cvs file, no column names
    dataframe = pd.read_csv(ratefile, header=None)

    # Set column names
    dataframe.columns = ['Currency', 'Rates']

    # set other column as index
    dataframe.set_index('Currency')

    # iteration over the list
    for row in dataframe.itertuples():

        # control currency
        if row.Currency == currency:

            # OK, found
            exchange_rate = row.Rates
            break

    return float(exchange_rate)

# Test
# print("Rate =", exchange("exchange_rates.csv", "GBP"))
# print("Rate =", exchange())


def transform(extract_data, ratefile = "exchange_rates.csv", currency = "GBP"):
    # Write your code here
    # extract data from cvs file
    exchange_rate = exchange(ratefile, currency)         # exchange(ratefile = "exchange_rates.csv", currency = "GBP")

    # transform data
    extract_data['Market Cap (US$ Billion)'] = round(extract_data['Market Cap (US$ Billion)'] * exchange_rate, 3)

    # rename columns
    extract_data.columns = ['Name', 'Market Cap (GBP$ Billion)']

    return extract_data

# Test
#extract_data = extract()
#print(transform(extract_data))


def load(data_to_load, targetfile = "data_to_save.csv"):
    '''
    Save the data to a CSV file.
    Default name of the CSV file ist data_to_save.csv
    '''
    data_to_load.to_csv(targetfile)


def log(message, logfile = "logfile.log"):
    '''
    Write the message in a log file.
    The default of log file name is logfile.log
    '''
    # Timestamp
    # Format: ISO = YYYY-mm-dd HH:MM:SS
    timestamp_format = '%Y-%h-%d %H:%M:%S'
    # get current timestamp
    now = datetime.now()
    # formated
    timestamp = now.strftime(timestamp_format)

    # open log file in append mode
    with open(logfile, "a") as f:
        # write the message into the log file
        # '\n' for a new line in the file
        f.write(timestamp + ',' + message + '\n')


#
#  ETL
#

# Set target file
targetfile = "test.csv"

# ETL process started
log("ETL process started")

log("Step Extract started")
# Call the Extract function
extracted_data = excract()
log("Step Extract ended")

log("Step Transform started")
# Call the Transform function
transform_data = transform(extract_data)
log("Step Transform ended")

log("Step Load started")
# Call the Load function
load(transformed_data, targetfile)
log("Step Load ended")

# ETL process ended
log("ETL process ended")
