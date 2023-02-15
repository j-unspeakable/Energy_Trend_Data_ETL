import pandas as pd

def process_excel_data(filename, sheet_name, header):
    df = pd.read_excel(f"./data/{filename}", sheet_name=sheet_name, header=header)
    df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    df.rename(columns=lambda x: x.replace('\n', '_'), inplace=True)
    df.set_index('Column1', inplace=True)
    df.index.name = None
    df['processed_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df['processed_date'] = pd.to_datetime(df['processed_date'])
    df['filename'] = filename
    print(df.head())
    return df
