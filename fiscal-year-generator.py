from myclasses.sql_executor import SQLExecutor
from sqlalchemy import create_engine
import pandas as pd

if __name__ == "__main__":
    # Create an instance of SQLExecutor
    sql_executor = SQLExecutor()

    # SQL query for CostReport data
    query = """
        SELECT * FROM CostReports;
        """

    # Execute the query and get the result as a DataFrame
    df = sql_executor.execute_query(query)


# Convert date columns to datetime objects
df['fiscal_start'] = pd.to_datetime(df['fiscal_start'])
df['fiscal_end'] = pd.to_datetime(df['fiscal_end'])

# Calculate fiscal range for each row
df['fiscal_range'] = (df['fiscal_end'] - df['fiscal_start']).dt.days.astype(int)

# Sort the DataFrame by 'prov_id' and 'fiscal_start'
df = df.sort_values(by=['prov_id', 'fiscal_start'])

# Initialize an empty list to store the indices of rows to remove
rows_to_remove = []

# Iterate over each row in the DataFrame
for i in range(1, len(df)):
    # Check if the fiscal period overlaps with the previous one for the same provider ID
    if df.iloc[i]['prov_id'] == df.iloc[i - 1]['prov_id']:
        # If the fiscal periods overlap, keep the one with the larger fiscal range
        if df.iloc[i]['fiscal_start'] < df.iloc[i - 1]['fiscal_end']:
            if df.iloc[i]['fiscal_range'] < df.iloc[i - 1]['fiscal_range']:
                rows_to_remove.append(i)
            else:
                rows_to_remove.append(i - 1)

# Drop the rows with smaller fiscal range in case of overlap
df = df.drop(rows_to_remove)
rows_to_remove = []

# Splitting the DataFrame
subset_greater_equal_364 = df[df['fiscal_range'] >= 364].reset_index(drop=True)
df = df[df['fiscal_range'] < 364].reset_index(drop=True)

# List of columns in the DataFrame that needed summed
columns_to_sum = ["tot_days_title_v", "tot_days_title_xviii", "tot_days_title_xix", "tot_days_title_other", "tot_days_title_tot", "tot_bed_days_avail", "tot_discharge_title_v", "tot_discharge_title_xviii", "tot_discharge_title_xix", "tot_discharge_title_other", "tot_discharge_tot", "snf_admis_title_v", "snf_admis_title_xviii", "snf_admis_title_xix", "snf_admis_other", "snf_admis_tot", "snf_days_title_v", "snf_days_title_xviii", "snf_days_title_xix", "snf_days_other", "snf_days_total", "snf_bed_days_avail", "snf_discharge_title_v", "snf_discharge_title_xviii", "snf_discharge_title_xix", "snf_discharge_title_other", "snf_discharge_tot", "nf_bed_days_avail", "nf_days_title_v", "nf_days_title_xix", "nf_days_other", "nf_days_total", "nf_discharge_title_v", "nf_discharge_title_xix", "nf_discharge_title_other", "nf_discharge_tot", "nf_admis_title_v", "nf_admis_title_xix", "nf_admis_other", "nf_admis_total", "tot_rug_days", "tot_salaries", "overhead_nonsalary_costs", "tot_charges", "tot_costs", "wagerelated_costs", "tot_salaries_adjusted", "contract_labor","total_general_inpatient_care_service", "inpatient_revenue", "tot_general_inpatient_revenue", "outpatient_revenue", "gross_revenue", "less_contractual_allowance", "net_patient_revenue", "total_operating_expense", "net_income_from_service_to_patients", "tot_other_income", "tot_income", "net_income", "inpatient_pps_amount", "nursing_allied_health_educ_activities","discount_on_patients"]

# Loop through the DataFrame and join rows together where the fiscal start of row one and fiscal end of row 2 are equivalent and sum to 364
for i in range(len(df) - 1):
    first_row = df.iloc[i]
    second_row = df.iloc[i + 1]

    # Check if prov_id matches
    if(first_row['prov_id'] == second_row['prov_id']):

        # Calculate the difference between the timestamp object
        report_end_time_difference = abs(first_row['fiscal_end'] - second_row['fiscal_start'])
        fiscal_range = abs(second_row['fiscal_end'] - first_row['fiscal_start'] )

        # Check if fiscal reports are chronologically consecutive and if aggregated the fiscal range would be 364 days
        if (report_end_time_difference <= pd.Timedelta(days=7) and fiscal_range.days in [363, 364, 365]):
            
            df.at[i + 1, 'fiscal_start'] = first_row['fiscal_start']
            
            # Sum columns for accounting period
            for column in columns_to_sum:
                first_row_c = first_row[column]
                second_row_c = second_row[column]

                if pd.isna(first_row_c) & pd.isna(second_row_c):
                    df.at[i + 1, column] = pd.NA
                elif pd.isna(first_row_c):
                    first_row_c = 0
                elif pd.isna(second_row_c):
                    second_row_c = 0
                else:
                    df.at[i + 1, column] = first_row_c + second_row_c
            
                
            # Store the first row to delete it after
            rows_to_remove.append(i)

# Remove marked rows        
df = df.drop(rows_to_remove).reset_index(drop=True)

# Recalculate fiscal_range
df['fiscal_range'] = (df['fiscal_end'] - df['fiscal_start']).dt.days.astype(int)

# Stack processed DataFrames and filter out the remaining values for processing
penultimate_df = pd.concat([subset_greater_equal_364 , df[df['fiscal_range'] >= 364].reset_index(drop=True)])
df = df[df['fiscal_range'] < 364].reset_index(drop=True)

rows_to_remove = []

# Run through the df one last time to aggregate reports up to 400 days.
for i in range(len(df) - 1):
    first_row = df.iloc[i]
    second_row = df.iloc[i + 1]

    # Check if prov_id matches
    if(first_row['prov_id'] == second_row['prov_id']):

        # Calculate the difference between the timestamp object
        report_end_time_difference = abs(first_row['fiscal_end'] - second_row['fiscal_start'])
        fiscal_range = abs(second_row['fiscal_end'] - first_row['fiscal_start'] )

        # Check if fiscal reports are chronologically consecutive and if aggregated the fiscal range would be 364 days
        if (report_end_time_difference <= pd.Timedelta(days=7) and fiscal_range.days < 400):
            # Add the first row to the remove list and aggregate the remaining data
            # Update fiscal_start of row2
            df.at[i + 1, 'fiscal_start'] = first_row['fiscal_start']
            
            # Sum columns for accounting period
            for column in columns_to_sum:
                first_row_c = first_row[column]
                second_row_c = second_row[column]

                if pd.isna(first_row_c) & pd.isna(second_row_c):
                    df.at[i + 1, column] = pd.NA
                elif pd.isna(first_row_c):
                    first_row_c = 0
                elif pd.isna(second_row_c):
                    second_row_c = 0
                else:
                    df.at[i + 1, column] = first_row_c + second_row_c
            
            # Store the first row to delete it after
            rows_to_remove.append(i)

# Remove marked rows and recalculate fiscal_range
df = df.drop(rows_to_remove).reset_index(drop=True)
df['fiscal_range'] = (df['fiscal_end'] - df['fiscal_start']).dt.days.astype(int)

# Stack remaining data for all reports over 300 days
final_df = pd.concat([penultimate_df, df[df['fiscal_range'] >= 300].reset_index(drop=True)])

# Add Fiscal Year for later timeseries use
def fiscal_year(date):

    if date.month > 8:
        return date.year + 1
    else:
        return date.year

final_df['fiscal_year'] = final_df['fiscal_start'].apply(fiscal_year)

# Method to chunk DataFrame for upload
def upload_dataframe_to_mysql(df, table_name, engine, chunk_size= 50000):
    try:
        # Drop the 'index' column if it exists
        df = df.drop(columns=['index'])
    except KeyError:
        pass  # 'index' column does not exist, continue without dropping

    # Calculate number of chunks
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size > 0 else 0)

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        df_chunk = df.iloc[start_idx:end_idx]

        try:
            # Upload chunk to MySQL table
            df_chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
            print(f'Chunk {i+1}/{num_chunks} uploaded successfully to MySQL table: {table_name}')
        except Exception as e:
            print(f'Error uploading chunk {i+1}/{num_chunks} to MySQL table: {e}')

# MySQL connection parameters
host= 'localhost'
user= 'josh'
password= 'go$T4GS'
database= 'data_4999'

# Create MySQL connection
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Upload the DataFrame to the server by chunk
upload_dataframe_to_mysql(final_df, 'aggCostReports', engine)

# Close the connection
engine.dispose()