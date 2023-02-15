## CONTENT
1. [ Overview ](#over)
2. [ Technical Requirements ](#Tech)
3. [ Deployment Steps ](#dep)
4. [ Conclusion ](#con)

<a name="over"></a>
## Overview
The data analysis team requires a new data pipeline for downloading and cleaning energy trend data. The data, which is available on the UK government's website, contains information about the supply and use of various energy sources. THe task is to develop a script that can automatically download and clean this data, validate its quality, and save it as a CSV file. The script should be scheduled to run daily to check for any new datasets, ensuring that the latest data is always available to the analyst. 

The analyst want you to start with the dataset "Supply and use of crude oil, natural gas liquids, and feedstocks" from the UK government's website: https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends. The data comes as an Excel spreadsheet with multiple tabs, where the analyst is only interested in the “Quarter” tab. 

The task is to perform the following operations:
1. Write a script that can check for new data, and if a new dataset is detected. Download the new 
Excel file.
2. Clean the data to remove any unnecessary information and to ensure that the data is in a 
consistent and well-structured format.
    - Missing values should be left blank

    - Ensure that any dates and timestamps are converted into a standard dateformat of `yyyy-MM-dd` and `yyyy-MM-dd HH:mm:ss` for timestamps if available.

    - Retain information about when the data was processed and the original filename
3. Validate the schema by checking that the data is in the same format and contains the same 
information as the previous dataset that has been downloaded.
4. Save the resulting DataFrame to a CSV file in a format that can be easily ingested into a data 
lake.

<a name="over"></a>
## Technical Requirements
1. Must be implemented as a Python PIP Package, using Python greater than or equal to V3.7.
    - If you are using Spark through a Notebook interface the Notebook should import the package
2. Must be implemented in either Pandas or PySpark. When using PySpark the package will be tested in a Databricks environment. 
3. The location of the CSV should be as a parameter to the package. 
    - If you implement your package in PySpark the solution should provide the tabular data in delta format as well for support.
4. Along with the CSV we expect two reports
    - CSV with data profiling for each incoming CSV file. The file should use the same naming convention as the CSV but have “_data_profiling” added to the end of the filename (before .csv)

        The minimum required profiling checks are:

        - Row count
        - Column count 
        - Min, max, median and mean for numerical columns 
        - Number of missing values

    - Data consistency report for each incoming CSV file summarizing the output of the checks. The report should use the same naming convention as the CSV but have `_data_consistency` added to the end of the filename.

        The minimum required data checks are:

        - Correct Time format
        - Number of missing values 
        - Any new columns from previous reports
        - Missing columns from previous reports

5. The solution must provide adequate logging for production support to diagnose any issues.

<a name="dep"></a>
## Deployment Steps
### Installation
To install the energyetl package, please follow these steps:
    
1. Create a new Python virtual environment and activate it. Conda or venv can be used.
    
    For venv:
    ```
    $ python -m venv env
    $ source env/bin/activate
    ```

    For conda:
     ```
    $ conda create -n env python=3.10 
    $ conda activate env
    ```

2. Clone this repository from GitHub:

    ```
    $ git clone https://github.com/j-unspeakable/Energy_Trend_Data_ETL.git
    ```

3. Navigate to the `energyetl` directory and install the package.
    ```
    $ cd energyetl
    $ pip install .
    ```

### Usage
To use the energyetl package, please follow this step:

1. Call `energyetl` from the terminal:

    ```
    energyetl
    ```

<a name="con"></a>
## Conclusion
This project involves the development of a Python PIP package that automatically downloads and cleans energy trend data from the UK government's website, specifically the "Supply and use of crude oil, natural gas liquids, and feedstocks" dataset from the "Quarter" tab. 

The script checks for new datasets and download them, clean the data, validate the schema, and save the resulting DataFrame to a CSV file that can be easily ingested into a data lake. 

The solution also provides two reports for each incoming CSV file: a data profiling report and a data consistency report and includes adequate logging for production support.