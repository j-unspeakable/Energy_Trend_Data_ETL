import os
import logging
from prefect import task

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not os.path.exists('./logs'):
    os.makedirs('./logs')

file_handler = logging.FileHandler('./logs/save_data.log')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

@task(log_prints=True)
def save_data_to_csv(df, filename):
    # Save the cleaned data as a CSV file.
    save_filename = os.path.splitext(filename)[0]
    save_csv = f'./data/{save_filename}.csv'
    df.to_csv(save_csv, index=False)
    logger.info(f"Data saved in the file {save_csv}")

    return save_filename