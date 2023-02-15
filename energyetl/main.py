from prefect import flow
from energyetl.ingest import ingest_excel_files
from energyetl.preprocess import process_excel_data
from energyetl.validation import validate_data
from energyetl.save_csv import save_data_to_csv
from energyetl.validation_report import generate_data_profiling_report, generate_data_consistency_report

@flow()
def main() -> None:
    '''
    Main function
    '''
    # Call ingest_excel_files function from ingest.py.
    sheet_name = 'Quarter'
    header = 4
    url = 'https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends'
    html_name = "Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 - quarterly)"
    filename = ingest_excel_files(url, html_name)

    # Call process_excel_data function from preprocess.py.
    df = process_excel_data(filename, sheet_name, header)

    # Call validate_data function from validation.py.
    previous_df = validate_data(filename, df, sheet_name, header)

    # Call save_data_to_csv function from save.py
    csv_filename = save_data_to_csv(df, filename)

    # Call generate_data_profiling_report function and generate_data_consistency_report function from validation_report.py.
    generate_data_profiling_report(df, csv_filename)
    generate_data_consistency_report(df, previous_df, csv_filename)

if __name__ == '__main__':
    main()