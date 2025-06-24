import pandas as pd
import numpy as np

# Load the Excel file
file_path = "/Users/michal.reuveni/Downloads/Job titles for solutions Power Bi dashboard.xlsx"  # Update with your file path
df = pd.read_excel(file_path, parse_dates=["Start date", "Termination date"])

# Define the date range
years = list(range(2016, 2029))
months = list(range(1, 13))
date_index = pd.MultiIndex.from_product([years, months], names=["Year", "Month"])

# Prepare a DataFrame to store accumulated employees per department
departments = df["Department"].unique()
accumulated_df = pd.DataFrame(index=date_index, columns=departments).fillna(0)


# Iterate over each employee and update the accumulated count
def calculate_active_employees(row):
    start_year, start_month = row["Start date"].year, row["Start date"].month
    end_year, end_month = (row["Termination date"].year, row["Termination date"].month) if pd.notna(
        row["Termination date"]) else (2028, 12)

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if (year > start_year or month >= start_month) and (
                    year < end_year or (year == end_year and month <= end_month)):
                accumulated_df.loc[(year, month), row["Department"]] += 1


# Apply the function to each employee
df.apply(calculate_active_employees, axis=1)

# Reset index for better readability
accumulated_df.reset_index(inplace=True)
# Save the results to an Excel file
output_path = "/Users/michal.reuveni/Downloads/employees.xlsx"  # Update with your desired save path
accumulated_df.to_excel(output_path, index=False)

print(f"Saved accumulated employee data to {output_path}")
