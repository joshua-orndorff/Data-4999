from myclasses.sql_executor import SQLExecutor
from sqlalchemy import create_engine
import pandas as pd
import warnings

# Disable the SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

if __name__ == "__main__":
    # Create an instance of SQLExecutor
    sql_executor = SQLExecutor()

    # Example SQL query
    query = """

        SELECT 
            a.prov_id,
            a.fiscal_year as year, 
            CAST(a.ownership AS CHAR) AS ownership,
            i.chow_last_12mos,
            i.state,
            i.city,
            i.county_ssa,
            i.zip,
            a.loc_type,
            CASE 
                WHEN a.outpatient_revenue IS NOT NULL THEN 'Y'
                ELSE 'N'
            END AS has_outpatient,
            a.num_beds,
            a.snf_num_beds,
            a.snf_admis_tot,
            a.tot_days_title_tot as tot_days,
            a.snf_days_total,
            a.tot_days_title_v,
            a.tot_days_title_xviii,
            a.tot_days_title_xix,
            a.tot_bed_days_avail,
            a.snf_avg_stay_len_title_tot,
            a.tot_discharge_tot,
            a.snf_discharge_tot,
            a.cash_on_hand_and_in_banks as cash,
            a.acct_rec,
            (a.tot_current_assets / NULLIF(a.total_assets, 0)) as pct_current_assets,
            (a.tot_fixed_assets / NULLIF(a.total_assets, 0)) as pct_fixed_assets,
            (a.total_other_assets / NULLIF(a.total_assets, 0)) as pct_other_assets,
            a.total_assets,
            a.acct_payable,
            a.mortgage_payable,
            a.notes_payable,
            a.unsecured_loans,
            (a.acct_payable + a.mortgage_payable + a.notes_payable + a.unsecured_loans) as total_debt,
            total_long_term_liabilities,
            (a.tot_current_liabilities / NULLIF(a.total_liabilities, 0)) as pct_current_liabilities,
            a.total_liabilities,
            a.tot_fund_balance,
            a.contract_labor,
            a.tot_salaries,
            a.overhead_nonsalary_costs,
            a.wagerelated_costs,
            a.tot_costs,
            a.total_operating_expense,
            a.buildings,
            a.tot_salaries_adjusted,
            (a.tot_current_assets / NULLIF(a.tot_current_liabilities, 0)) as current_ratio,
            (a.cash_on_hand_and_in_banks / NULLIF(a.tot_current_liabilities, 0)) as quick_ratio,
            (a.gross_revenue / NULLIF(a.tot_fixed_assets, 0)) as fixed_asset_to_ratio,
            (a.gross_revenue / NULLIF(a.total_assets, 0)) as asset_to_ratio,
            d.def_score,
            i.fine_cnt,
            i.fine_tot,
            i.tot_penlty_cnt,
            i.resfamcouncil,
            i.sprinkler_status,
            i.overall_rating,
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
            i.snfs_in_city,
            a.inpatient_revenue,
            a.net_patient_revenue,
            a.net_income,
            a.tot_income,
            (a.gross_revenue - a.total_operating_expense) as operating_income,
            a.gross_revenue,
            (i.totlichrd / NULLIF(a.net_income_from_service_to_patients, 0)) as lic_staff_income_ratio,
            (a.inpatient_revenue / NULLIF(a.num_beds, 0)) as inpatient_revenue_per_bed,
            ((a.tot_income - a.total_operating_expense) / NULLIF(a.gross_revenue, 0)) as operating_margin,
            (a.net_income / NULLIF(a.gross_revenue, 0)) as profit_margin,
            (a.net_income / NULLIF(a.total_assets, 0)) as roa,
            (i.totlichrd / i.tothrd) as makeup
            

        FROM aggCostReports a

        LEFT JOIN (
                SELECT * FROM DeficiencyScore
            ) d ON a.prov_id = d.prov_id AND a.fiscal_year = d.fiscal_year
            
        LEFT JOIN (
                SELECT * FROM ProviderInfo
            ) i ON a.prov_id = i.prov_id AND a.fiscal_year = LEFT(i.filedate,4);
        """

    # Execute the query and get the result as a DataFrame
    df_init = sql_executor.execute_query(query)
    
    query= """
        SELECT zip, over_70_pct, pop_over_70  FROM Over70 WHERE year = 2020;
        """

    # Execute the query and get the result as a DataFrame
    df_pop = sql_executor.execute_query(query)

# Join pop data into the main df
df_pop['zip'] = pd.to_numeric(df_pop['zip'], errors='coerce')
df_init = pd.merge(df_init, df_pop, on=['zip'], how='left')

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

def get_political_lean(state_code):
    political_leanings = {
        'al': 'Conservative',
        'ak': 'Mixed',
        'az': 'Mixed',
        'ar': 'Conservative',
        'ca': 'Liberal',
        'co': 'Mixed',
        'ct': 'Liberal',
        'de': 'Liberal',
        'dc': 'Liberal',
        'fl': 'Mixed',
        'ga': 'Conservative',
        'hi': 'Liberal',
        'id': 'Conservative',
        'il': 'Liberal',
        'in': 'Conservative',
        'ia': 'Mixed',
        'ks': 'Conservative',
        'ky': 'Conservative',
        'la': 'Conservative',
        'me': 'Mixed',
        'md': 'Liberal',
        'ma': 'Liberal',
        'mi': 'Mixed',
        'mn': 'Liberal',
        'ms': 'Conservative',
        'mo': 'Conservative',
        'mt': 'Conservative',
        'ne': 'Conservative',
        'nv': 'Mixed',
        'nh': 'Mixed',
        'nj': 'Liberal',
        'nm': 'Liberal',
        'ny': 'Liberal',
        'nc': 'Mixed',
        'nd': 'Conservative',
        'oh': 'Mixed',
        'ok': 'Conservative',
        'or': 'Liberal',
        'pa': 'Mixed',
        'pr': 'Liberal',
        'ri': 'Liberal',
        'sc': 'Conservative',
        'sd': 'Conservative',
        'tn': 'Conservative',
        'tx': 'Conservative',
        'ut': 'Conservative',
        'vt': 'Liberal',
        'va': 'Mixed',
        'wa': 'Liberal',
        'wv': 'Conservative',
        'wi': 'Mixed',
        'wy': 'Conservative'
    }
    
    return political_leanings.get(state_code, 'Unknown')

# Assuming df_init is your DataFrame
df_init['state_lean'] = df_init['state'].apply(get_political_lean)


def get_ownership(ownership_code):
    ownership_mapping = {
        '1' : 'Nonprofit',
        '2': 'Nonprofit',
        '3': 'Propietary',
        '4': 'Propietary',
        '5': 'Propietary',
        '6': 'Propietary',
        '7': 'Propietary',
        '8': 'Government',
        '10': 'Government',
        '11': 'Government',
        '12': 'Government',
        '13': 'Government',
    }
    
    return ownership_mapping.get(ownership_code, 'Unknown')

# Assuming 'ownership' column contains the ownership codes
df_init.loc[:, 'ownership_type'] = df_init['ownership'].apply(get_ownership)

# Drop observations where null in netincome
df = df_init.dropna(subset=['net_income','gross_revenue'])
numerical_columns = df.select_dtypes(include=['number']).columns


# Conditions
condition1 = df['certification'] == 'medicare'
condition2 = df['certification'] == 'medicare and medicaid'
condition3 = df['certification'] == 'medicaid'

# Create new columns
df.loc[:, 'medicare'] = (condition1 | condition2).astype(bool)
df.loc[:, 'medicaid'] = (condition2 | condition3).astype(bool)

df.drop(columns=['certification'])


def snf_size(data):

    if data >= 174:
        return 'Large(174)'
    elif data >= 102:
        return 'Medium(102)'
    else:
        return 'Small(52)'
    
df.loc[:, 'snf_size'] = df['num_beds'].apply(snf_size)


# Calculate percentiles once
makeup_33 = df['makeup'].quantile(0.33)
makeup_66 = df['makeup'].quantile(0.66)
stay_length_33 = df['snf_avg_stay_len_title_tot'].quantile(0.33)
stay_length_66 = df['snf_avg_stay_len_title_tot'].quantile(0.66)

def staff_style(data):
    if data <= makeup_33:
        return 'low_license'
    elif data <= makeup_66:
        return 'med_license'
    else:
        return 'high_license'
    
df.loc[:, 'staff_style'] = df['makeup'].apply(staff_style)

def stay_length(data):
    if data <= stay_length_33:
        return 'short_term'
    elif data <= stay_length_66:
        return 'med_term'
    else:
        return 'long_term'
    
df.loc[:, 'fac_type'] = df['snf_avg_stay_len_title_tot'].apply(stay_length)

# Reduce zip for binning
df.loc[:, 'zip'] = (df['zip'] // 1000).astype(str)

df.loc[:, 'weighted_all_cycles_score'] = pd.to_numeric(df['weighted_all_cycles_score'], errors='coerce')

# Pull difference ratios
df.loc[:, 'nf_discharge_tot'] = df['tot_discharge_tot'] - df['snf_discharge_tot']
df.loc[:, 'nf_num_beds'] =  df['num_beds']  - df['snf_num_beds']
df.loc[:, 'totlichrd_to_tot'] = df['totlichrd'] / df['tothrd']    

# Normalize data using number of beds to control for size
columns = [
    'snf_admis_tot',
    'tot_days',
    'snf_days_total',
    'tot_days_title_v',
    'tot_days_title_xviii',
    'tot_days_title_xix',
    'tot_bed_days_avail',
    'tot_discharge_tot',
    'snf_discharge_tot',
    'cash',
    'acct_rec',
    'total_assets',
    'acct_payable',
    'total_liabilities',
    'contract_labor',
    'tot_salaries',
    'overhead_nonsalary_costs',
    'wagerelated_costs',
    'tot_costs',
    'total_operating_expense',
    'buildings',
    'fine_cnt',
    'fine_tot',
    'snf_admis_tot',
    'snf_num_beds',
    'nf_num_beds',
    'bedcert',
    'tot_fund_balance'
]

for col in columns:
    # Perform division of the column by 'num_beds' without creating a new column
    df.loc[:, col] = df[col] / df['num_beds']

df = df.drop_duplicates(subset=['prov_id', 'year'])

# MySQL connection parameters
host= 'localhost'
user= 'josh'
password= 'go$T4GS123'
database= 'data_4999'

# Create MySQL connection
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Define total number of rows and chunk size
total_rows = len(df)
chunk_size = total_rows // 4  # Divide total rows into 4 equal chunks

# Upload the DataFrame to MySQL in 4 chunks
for i in range(0, total_rows, chunk_size):
    df_chunk = df[i:i+chunk_size]  # Get the current chunk
    df_chunk.to_sql('Master', con=engine, if_exists='append', index=False)

# Close the connection
engine.dispose()