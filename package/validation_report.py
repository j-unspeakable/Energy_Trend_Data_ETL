import os
import pandas as pd
import pandas_profiling


def generate_data_profiling_report(df: pd.DataFrame, save_filename):

    # Generate standard HTML profiling report with pandas_profiling
    # Generate data profile and save to file
    if not os.path.exists('./report'):
        os.makedirs('./report')
    profile = pandas_profiling.ProfileReport(df, minimal=True)
    profile.to_file(f"./report/{save_filename}_data_profiling.html")

    # Generate another profiling report in csv. 
    # Calculate the number of missing values per column
    missing_values = df.isna().sum()
    
    # Calculate the row count and column count
    row_count = len(df)
    column_count = len(df.columns)
    
    # Combine the results of `df.describe()` with the additional stats
    description = df.describe(include='all')
    description.loc['missing_values', :] = missing_values
    description.loc['row_count', :] = row_count
    description.loc['column_count', :] = column_count

    save_path = f"./report/{save_filename}_data_profiling.csv"
    description.to_csv(save_path)
    
    return save_path

def generate_data_consistency_report(df: pd.DataFrame, previous_df: pd.DataFrame, save_filename):
    report = {}

    if not os.path.exists('./report'):
        os.makedirs('./report')
    # Check for correct time format
    time_cols = ['processed_date']
    for col in time_cols:
        if not pd.api.types.is_datetime64_dtype(df[col]):
            report[col] = f"Column {col} has incorrect time format"
        else:
            report[col] = f"Column {col} has correct time format"

    # Check for missing values
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            report[col] = f"Column {col} has {df[col].isnull().sum()} missing value(s)"
        else:
            report['missing_value(s)'] = "No column has missing value(s)"

    # Check for new columns
    new_cols = set(df.columns) - set(previous_df.columns)
    if new_cols:
        report['new_columns'] = f"New column(s) detected: {new_cols}"
    else:
        report['new_columns'] = "No new column(s) detected"
    
    # Check for missing columns
    missing_cols = set(previous_df.columns) - set(df.columns)
    if missing_cols:
        report['missing_columns'] = f"Missing columns detected: {missing_cols}"
    else:
        report['missing_columns'] = f"No missing column(s) detected"
    
    # Save report to file
    report_df = pd.DataFrame(report.items(), columns=['Check', 'Result'])
    save_path = f"./report/{save_filename}_data_consistency.csv"
    report_df.to_csv(save_path, index=False)

    return save_path