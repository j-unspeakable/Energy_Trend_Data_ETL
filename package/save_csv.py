import os

def save_data_to_csv(df, filename):
    # Save the cleaned data as a CSV file.
    save_filename = os.path.splitext(filename)[0]
    df.to_csv(f'./data/{save_filename}.csv', index=False)

    return save_filename