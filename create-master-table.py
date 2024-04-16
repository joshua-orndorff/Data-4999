from myclasses.sql_executor import SQLExecutor
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    # Create an instance of SQLExecutor
    sql_executor = SQLExecutor()

    # Example SQL query
    query = """

        SELECT 
        
            a.prov_id,
            a.fiscal_year, 
            a.ownership,
            a.tot_days_title_tot as tot_days,
            a.tot_days_title_v,
            a.tot_days_title_xviii,
            a.tot_days_title_xix,
            a.num_beds,
            a.tot_bed_days_avail,
            a.snf_num_beds,
            a.snf_admis_tot,
            a.snf_avg_stay_len_title_tot,
            a.snf_days_total,
            a.tot_discharge_tot,
            a.snf_discharge_tot,
            a.tot_salaries,
            a.overhead_nonsalary_costs,
            a.cash_on_hand_and_in_banks as cash,
            a.acct_rec,
            a.acct_payable,
            a.tot_current_assets,
            a.tot_fixed_assets,
            a.tot_assets,
            a.tot_liabilities,
            a.contract_labor,
            a.net_patient_revenue,
            a.gross_revenue,
            a.general_fund_balance,
            a.tot_fund_balance,
            a.wagerelated_costs,
            a.total_operating_expense,
            a.loc_type,
            a.buildings,
            a.current_ratio,
            a.employee_inpatient_revenue_ratio,
            a.revenue_per_bed,
            a.fixed_asset_TO_ratio,
            a.total_asset_TO_ratio,
            a.operating_margin,
            a.profit_margin,
            d.num_scope as deficiency_score,
            i.overall_rating,
            i.county_ssa,
            i.zip,
            i.resfamcouncil,
            i.sprinkler_status,
            i.chow_last_12mos,
            i.fine_cnt,
            i.fine_tot,
            i.tot_penlty_cnt,
            i.state,
            i.survey_rating,
            i.quality_rating,
            i.staffing_rating,
            i.rn_staffing_rating,
            i.aidhrd,
            i.vochrd,
            i.rnhrd,
            i.totlichrd,
            i.tothrd,
            i.pthrd,
            i.weighted_all_cycles_score,
            i.certification,
            i.bedcert,
            a.net_income

        FROM aggCostReports a

        LEFT JOIN (
                SELECT * FROM DeficiencyYear
            ) d ON a.prov_id = d.prov_id AND a.fiscal_year = d.report_year
            
        LEFT JOIN (
                SELECT * FROM ProviderInfo
            ) i ON a.prov_id = i.prov_id AND a.fiscal_year = LEFT(i.filedate,4);

        """

    # Execute the query and get the result as a DataFrame
    df_init = sql_executor.execute_query(query)

def remove_outliers_iqr(df, columns):
    for column in columns:
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df

#Set columns to remove outliers
cols = ['tot_assets', 'snf_avg_stay_len_title_tot', 'tot_discharge_tot', 
        'snf_discharge_tot', 'cash', 'current_ratio', 
        'employee_inpatient_revenue_ratio', 'net_income']

# Call the function to remove outliers from the DataFrame
df_init = remove_outliers_iqr(df_init, cols)

def get_region(state_code):
    regions = {
        'al': 'Southeast',
        'ak': 'Pacific Northwest',
        'az': 'Southwest',
        'ar': 'South',
        'ca': 'Pacific',
        'co': 'Mountain West',
        'ct': 'New England',
        'de': 'Mid-Atlantic',
        'dc': 'Mid-Atlantic',
        'fl': 'Southeast',
        'ga': 'Southeast',
        'hi': 'Pacific',
        'id': 'Mountain West',
        'il': 'Midwest',
        'in': 'Midwest',
        'ia': 'Midwest',
        'ks': 'Midwest',
        'ky': 'South',
        'la': 'South',
        'me': 'New England',
        'md': 'Mid-Atlantic',
        'ma': 'New England',
        'mi': 'Midwest',
        'mn': 'Midwest',
        'ms': 'South',
        'mo': 'Midwest',
        'mt': 'Mountain West',
        'ne': 'Midwest',
        'nv': 'Mountain West',
        'nh': 'New England',
        'nj': 'Mid-Atlantic',
        'nm': 'Southwest',
        'ny': 'Mid-Atlantic',
        'nc': 'Southeast',
        'nd': 'Midwest',
        'oh': 'Midwest',
        'ok': 'South',
        'or': 'Pacific Northwest',
        'pa': 'Mid-Atlantic',
        'pr': 'Puerto Rico',
        'ri': 'New England',
        'sc': 'Southeast',
        'sd': 'Midwest',
        'tn': 'South',
        'tx': 'South',
        'ut': 'Mountain West',
        'vt': 'New England',
        'va': 'Mid-Atlantic',
        'wa': 'Pacific Northwest',
        'wv': 'Mid-Atlantic',
        'wi': 'Midwest',
        'wy': 'Mountain West'
    }
    
    return regions.get(state_code, 'Unknown')

df_init['region'] = df_init['state'].apply(get_region)

def get_ownership(ownership_code):
    ownership_mapping = {
        1. : 'Nonprofit',
        2. : 'Nonprofit',
        3. : 'Propietary',
        4. : 'Propietary',
        5. : 'Propietary',
        6. : 'Propietary',
        7. : 'Propietary',
        8. : 'Government',
        9. : 'Government',
        10. : 'Government',
        11. : 'Government',
        12. : 'Government',
        13. : 'Government',
    }
    
    return ownership_mapping.get(ownership_code, 'Unknown')

# Assuming 'ownership' column contains the ownership codes
df_init['ownership'] = df_init['ownership'].apply(get_ownership)

# Drop observations where null in netincome
df = df_init.dropna(subset=['net_income'])
numerical_columns = df.select_dtypes(include=['number']).columns
df[numerical_columns] = df[numerical_columns].fillna(df[numerical_columns].median())

def snf_size(data):

    if data >= 174:
        return 'Large(174)'
    elif data >= 102:
        return 'Medium(102)'
    else:
        return 'Small(52)'
    
df['snf_size'] = df['num_beds'].apply(snf_size)

# Replace NaN values with median for numerical columns
numerical_columns = df.select_dtypes(include=['number']).columns
df[numerical_columns] = df[numerical_columns].fillna(df[numerical_columns].median())
df['weighted_all_cycles_score'] = pd.to_numeric(df['weighted_all_cycles_score'], errors='coerce')

# Sent to csv
df.to_csv('~/Documents/Repositories/Data 4999/BAC@MC 2024 Phase One Datasets/master.csv')

# MySQL connection parameters
host= 'localhost'
user= 'josh'
password= 'go$T4GS'
database= 'data_4999'

# Create MySQL connection
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Upload the yearly deficiency score to SQL
df.to_sql('Master', con=engine, if_exists='replace', index=False)

# Close the connection
engine.dispose()