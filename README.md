## Project Structure

### Folders

- **Power Query**
  - Contains the file `power_query_test.xlsx`, which is used for data processing tasks related to Power Query.

- **Python**
  - Contains several files, including the main script `main_code.py`. This script executes the primary tasks outlined below and contains necessary instructions for installation and usage. Other files in this folder are either source files or generated output files required for or produced by the tasks.

## Python Tasks Overview

### Task 1:
The banks' statements are generated by code and saved to the `banks_statements.csv` file.

### Task 2:
Check if all operations are present in the company register. The result is saved in the 
register_fullness.csv file generated by the code. The answer is `Yes` since all values in 
the `is_present_in_register` column are 'True'.

### Task 3:
Commissions for some operations are incorrect, as indicated in the `is_correct_commission`
column of the `commissions.csv` file. However, the discrepancies in the commissions are 
relatively minor.

### Task 4:
The financial report data is generated in the financial_report.xlsx file and divided into 
different sheets, `Report 1` and `Report 2`. These reports are also displayed in the terminal.

## Installation

To run the Python scripts, you need to install the required Python modules. The primary module required is `forex-python`, which can be installed via pip:

```bash
pip install forex-python
```

## Outputs

Banks Statements: `banks_statements.csv`
Register Fullness Check: `register_fullness.csv`
Commissions Validation: `commissions.csv`
Financial Reports: `financial_report.xlsx` with "Report 1" and "Report 2" sheets
