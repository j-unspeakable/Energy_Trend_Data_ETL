import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not os.path.exists('./logs'):
    os.makedirs('./logs')

file_handler = logging.FileHandler('./logs/preprocess_data.log')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def process_excel_data(filename, sheet_name, header):
    df = pd.read_excel(f"./data/{filename}", sheet_name=sheet_name, header=header)
    df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    df.rename(columns=lambda x: x.replace('\n', '_'), inplace=True)
    df.set_index('Column1', inplace=True)
    df.index.name = None
    df['processed_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df['processed_date'] = pd.to_datetime(df['processed_date'])
    df['filename'] = filename
    logger.info(df.head())
    return df