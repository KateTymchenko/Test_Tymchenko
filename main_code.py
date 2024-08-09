# Modules to install
# pip install forex-python
# Task 1: The Banks' statements are generating by code to 'banks_statements.csv'
# Task 2: Check if the all operations are present in the company register. Result in
#         the 'register_fullness.csv' file generated bu code. The answer is 'Yes' as
#         all values in "is_present_in_register" column are True
# Task 3:


import pandas as pd
import zipfile
import csv
import openpyxl
import os

import io
from detect_delimiter import detect
from itertools import islice
from datetime import datetime
from forex_python.converter import CurrencyCodes


def main():

    #### TASK 1 ####
    # Read the banks' data
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

    # Perform Validation
    perform_validation(df, "Validation of the banks")

    # Casting columns to desired types
    df = df.astype(
        {
            "account_name": "string",
            "datetime": "datetime64[ns]",  # datetime type
            "transaction_id": "string",
            "provider_name": "string",
            "client_name": "string",
            "currency": "string",
            "credit": "float64",  # casting numeric columns to float
            "debit": "float64",  # casting numeric columns to float
            "commission": "float64",  # casting numeric columns to float
            "description": "string",
        }
    )

    # Read the 'register' file into a DataFrame
    register = pd.read_csv("register.csv")
    register["provider_name"] = register["provider_name"].astype(str)

    # Perform Validation
    perform_validation(register, "Validation of the register")

    # Casting columns to desired types
    register = register.astype(
        {
            "account_name": "string",
            "datetime": "datetime64[ns]",  # datetime type
            "transaction_id": "string",
            "provider_id": "string",
            "provider_name": "string",
            "operation_type": "string",
            "currency": "string",
            "amount": "float64",  # casting numeric columns to float
            "commission": "float64",  # casting numeric columns to float
            "commentary": "string",
        }
    )

    # Save the validated banks statements to 'banks_statements.csv'
    # check if file exists
    if os.path.exists("banks_statemetns.csv"):
        os.remove("banks_statemetns.csv")
    df.to_csv("banks_statemetns.csv")

    #### TASK 2 ####
    # Prepare the `account_name` column by removing the first 4 characters
    register["account_name"] = register["account_name"].str[:-4]

    # Sort the DataFrame by 'client_name', 'provider_name', 'currency', and 'datetime'
    df = df.sort_values(by=["client_name", "provider_name", "currency", "datetime"])

    # Group by 'client_name', 'provider_name', 'currency' and get the next row's 'debit' value
    df["next_debit"] = df.groupby(["client_name", "provider_name", "currency"])[
        "debit"
    ].shift(-1)
    # Group by 'client_name', 'provider_name', 'currency' and get the next row's 'commission' value
    df["next_commission"] = df.groupby(["client_name", "provider_name", "currency"])[
        "commission"
    ].shift(-1)

    # Identify the rows where the 'credit' matches the next row's 'debit'
    matching_rows = df["credit"] == df["next_debit"]

    # Replace the 'commission' column with the next commission value where a match is found
    df.loc[matching_rows, "commission"] = df.loc[matching_rows, "next_commission"]

    # Drop the 'next_commission' and 'next_debit' columns as it's no longer needed
    df = df.drop(columns=["next_commission", "next_debit"])

    # Filter df to include only rows where credit > 0
    df = df[df["credit"] > 0]

    # Perform the left join based on the conditions
    merged_df = pd.merge(
        df,
        register,
        how="left",
        left_on=["client_name", "provider_name", "currency", "credit", "datetime"],
        right_on=["account_name", "provider_name", "currency", "amount", "datetime"],
        suffixes=("_banks", "_register"),
    )

    # Add "is_present_in_register" column to indicate if the transaction was found
    merged_df["is_present_in_register"] = ~merged_df["transaction_id_register"].isna()

    # Save the register fullness result to 'register_fullness.csv'
    # check if file exists
    if os.path.exists("register_fullness.csv"):
        os.remove("register_fullness.csv")
    merged_df.to_csv("register_fullness.csv")

    #### TASK 3 ####
    

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
    # Check if provider_name is a string
    return isinstance(name, str)


def validate_client_name(name):
    # Check if client_name is not empty and is a string
    return isinstance(name, str)


def validate_currency(currency):
    # Check if currency is in a list of valid currencies
    currency_codes = CurrencyCodes()
    return currency_codes.get_currency_name(currency) is not None


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


def validate_operation_type(op_type):
    # Check if operation_type is either 'outcome' or 'income'
    return op_type in ["outcome", "income"]


def validate_commentary(commentary):
    # Check if commentary is a string (it can be empty)
    return isinstance(commentary, str)


def validate_provider_id(provider_id):
    # Check if the value is a float or can be converted to a float
    try:
        float(provider_id)
        return True
    except ValueError:
        return False


# Perform validation
def perform_validation(df, df_type):
    validation_errors = []

    for index, row in df.iterrows():
        if not validate_account_name(row["account_name"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid account_name '{row['account_name']}'"
            )

        if not validate_datetime(row["datetime"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid datetime '{row['datetime']}'"
            )

        if not validate_transaction_id(row["transaction_id"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid transaction_id '{row['transaction_id']}'"
            )

        if not validate_provider_name(row["provider_name"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid provider_name '{row['provider_name']}'"
            )

        if not validate_currency(row["currency"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid currency '{row['currency']}'"
            )

        if "provider_id" in row and not validate_provider_id(row["provider_id"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid provider_id '{row['provider_id']}'"
            )

        if "operation_type" in row and not validate_operation_type(
            row["operation_type"]
        ):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid operation_type '{row['operation_type']}'"
            )

        if "amount" in row and not validate_amount(row["amount"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid amount '{row['amount']}'"
            )

        if "debit" in row and not validate_amount(row["debit"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid debit '{row['debit']}'"
            )

        if "credit" in row and not validate_amount(row["credit"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid credit '{row['credit']}'"
            )

        if not validate_amount(row["commission"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid commission '{row['commission']}'"
            )

        if "commentary" in row and not validate_commentary(row["commentary"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid commentary '{row['commentary']}'"
            )

        if "client_name" in row and not validate_client_name(row["client_name"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid client_name '{row['client_name']}'"
            )

        if "description" in row and not validate_description(row["description"]):
            validation_errors.append(
                f"{df_type} Row {index}: Invalid description '{row['description']}'"
            )

    if validation_errors:
        print(f"{df_type}: Validation errors found:")
        for error in validation_errors:
            print(error)
    else:
        print(f"{df_type}: All data is valid.")


# Entry point of the script
if __name__ == "__main__":
    main()
