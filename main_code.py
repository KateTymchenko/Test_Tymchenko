# Modules to install
# pip install forex-python

import pandas as pd
import zipfile
import csv
import openpyxl
import io
from detect_delimiter import detect
from itertools import islice
from datetime import datetime
from forex_python.converter import CurrencyCodes

def main():
    # Read the 'register' file into a DataFrame
    register = pd.read_csv("register.csv")

    # Initialize empty lists to store data from the zip files
    account_name = []
    datetime = []
    transaction_id = []
    provider_name = []
    client_name = []
    currency = []
    debit = []
    credit = []
    commission = []
    description = []

    # Unzip the file 'employee_task_statements.zip'
    with zipfile.ZipFile("employee_task_statements.zip") as myzip:
        for file in myzip.namelist():
            # Detect the delimiter (',' or ';') by analyzing the file's content
            with myzip.open(file) as f0:
                del_1 = 0
                del_2 = 0
                for line in f0:
                    del_1 += str(line).count(",")
                    del_2 += str(line).count(";")
                # Choose the delimiter with more occurrences
                if del_1 > del_2:
                    del_ = ","
                else:
                    del_ = ";"
            
            # Read the file and skip the first 5 lines to get the header from the 6th line
            with myzip.open(file, "r") as f:
                reader = csv.reader(io.TextIOWrapper(f), delimiter=del_)
                iterator = islice(reader, 5, None)
                headers = next(iterator)
            
            # Read the file again to parse the data using the detected header
            with myzip.open(file, "r") as f1:
                dict_reader = csv.DictReader(
                    io.TextIOWrapper(f1, encoding="utf-8"),
                    delimiter=del_,
                    fieldnames=headers,
                )

                # Skip the first 5 lines and start reading data
                iterator1 = islice(dict_reader, 5, None)

                # Loop through each row in the file
                for col in iterator1:
                    # Append the values to respective lists
                    account_name.append(col["account_name"])
                    datetime.append(col["datetime"])
                    transaction_id.append(col["transaction_id"])
                    provider_name.append(col["provider_name"])
                    client_name.append(col["client_name"])
                    currency.append(col["currency"])
                    commission.append(col["commission"])

                    # Validate fields from various files to ensure consistency in common columns
                    # Handle credit and debit fields based on the 'Debi/Credit' column
                    if "Debi/Credit" in col:
                        if col["Debi/Credit"] == "D":
                            debit.append(col["amount"])
                            credit.append(0)
                        elif col["Debi/Credit"] == "C":
                            credit.append(col["amount"])
                            debit.append(0)
                    
                    # Append debit and credit values if they exist
                    if "debit" in col:
                        debit.append(col["debit"])
                    if "credit" in col:
                        credit.append(col["credit"])
                    
                    # Handle description or payment info fields
                    if "payment info" in col:
                        description.append([col["payment info"]])
                    elif "description" in col:
                        description.append([col["description"]])


    # Create a DataFrame from the lists
    df = pd.DataFrame(
        {
            "account_name": account_name,
            "datetime": datetime,
            "transaction_id": transaction_id,
            "provider_name": provider_name,
            "client_name": client_name,
            "currency": currency,
            "credit": credit,
            "debit": debit,
            "commission": commission,
            "description": description,
        }
    )

    # Replace the ',' symbol in 'commission' with a '.' for proper numeric parsing
    df["commission"] = df["commission"].str.replace(",", ".", regex=False)

    # Convert the description list to a string, removing brackets
    df["description"] = df["description"].apply(lambda x: ", ".join(x))

    perform_validation(df)
    #df.to_excel('out.xlsx')

# Define validation functions to check each field's format and value

def validate_account_name(name):
    # Check if account_name is not empty and is a string
    return isinstance(name, str) and len(name) > 0

def validate_datetime(date_str):
    # Validate datetime format 'YYYY-MM-DD HH:MM:SS'
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False

def validate_transaction_id(tid):
    # Check if transaction_id is a non-empty string
    return isinstance(tid, str) and len(tid) > 0

def validate_provider_name(name):
    # Check if provider_name is not empty and is a string
    return isinstance(name, str) and len(name) > 0

def validate_client_name(name):
    # Check if client_name is not empty and is a string
    return isinstance(name, str)

def validate_currency(currency):
    # Check if currency is in a list of valid currencies
    currency_codes = CurrencyCodes()
    return currency_codes.get_currency_name(currency)

def validate_amount(value):
    # Check if the value is a float or can be converted to a float
    try:
        float(value)
        return True
    except ValueError:
        return False

def validate_description(desc):
    # Check if description is a string (it can be empty)
    return isinstance(desc, str)

def perform_validation(df):
    # Perform validation on the DataFrame and collect any errors
    validation_errors = []

    for index, row in df.iterrows():
        if not validate_account_name(row["account_name"]):
            validation_errors.append(
                f"Row {index}: Invalid account_name '{row['account_name']}'"
            )

        if not validate_datetime(row["datetime"]):
            validation_errors.append(
                f"Row {index}: Invalid datetime '{row['datetime']}'"
            )

        if not validate_transaction_id(row["transaction_id"]):
            validation_errors.append(
                f"Row {index}: Invalid transaction_id '{row['transaction_id']}'"
            )

        if not validate_provider_name(row["provider_name"]):
            validation_errors.append(
                f"Row {index}: Invalid provider_name '{row['provider_name']}'"
            )

        if not validate_client_name(row["client_name"]):
            validation_errors.append(
                f"Row {index}: Invalid client_name '{row['client_name']}'"
            )

        if not validate_currency(row["currency"]):
            validation_errors.append(
                f"Row {index}: Invalid currency '{row['currency']}'"
            )

        if not validate_amount(row["credit"]):
            validation_errors.append(
                f"Row {index}: Invalid credit amount '{row['credit']}'"
            )

        if not validate_amount(row["debit"]):
            validation_errors.append(
                f"Row {index}: Invalid debit amount '{row['debit']}'"
            )

        if not validate_amount(row["commission"]):
            validation_errors.append(
                f"Row {index}: Invalid commission amount '{row['commission']}'"
            )

        if not validate_description(row["description"]):
            validation_errors.append(
                f"Row {index}: Invalid description '{row['description']}'"
            )

    # Check if there were any validation errors
    if validation_errors:
        print("Validation errors found:")
        for error in validation_errors:
            print(error)
    else:
        print("All data is valid.")

# Entry point of the script
if __name__ == "__main__":
    main()
