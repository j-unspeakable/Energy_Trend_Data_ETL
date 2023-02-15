import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not os.path.exists('./logs'):
    os.makedirs('./logs')

file_handler = logging.FileHandler('./logs/validate_data.log')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def validate_data(filename, df, sheet_name, header):
    # read the previous unprocessed dataset
    previous_df = pd.read_excel(f"./data/{filename}", sheet_name=sheet_name, header=header)

    previous_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    previous_df.rename(columns=lambda x: x.replace('\n', '_'), inplace=True)
    previous_df.set_index('Column1', inplace=True)

    # subset the columns of interest from the previous data. The last two columns were excluded because they were engineered.
    previous_df = previous_df[df.columns[: -2]]
    new_df = df[df.columns[: -2]]

    # check if the columns in both datasets are the same
    if set(previous_df.columns) != set(new_df.columns):
        logger.warning("Columns in previous and new data don't match")
    else:
        logger.info("Columns in previous and new data match")

    # check if the data types in both datasets are the same
    if not all(previous_df.dtypes == new_df.dtypes):
        logger.warning("Data types in previous and new data don't match")
    else:
        logger.info("Data types in previous and new data match")

    # check if the number of rows in both datasets are the same
    if previous_df.shape[0] != new_df.shape[0]:
        logger.warning("Number of rows in previous and new data don't match")
    else:
        logger.info("Number of rows in previous and new data match")

    # check if the values in both datasets are the same
    if not previous_df.equals(new_df):
        logger.warning("Values in previous and new data don't match")
    else:
        logger.info("Values in previous and new data match")

    return previous_df