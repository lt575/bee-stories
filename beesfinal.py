# Data


*   Monthly temperatures-https://www.kaggle.com/datasets/justinrwong/average-monthly-temperature-by-us-state
*   Pollination Data-https://springernature.figshare.com/articles/dataset/PolLimCrop_dataset/24625299?backTo=%2Fcollections%2FPolLimCrop_-_A_global_database_of_Pollen_Limitation_in_Crops%2F6640595&file=43269153
*   Honey Production-https://www.kaggle.com/datasets/jessicali9530/honey-production/data
*   Fruit and Vegetable Prices-https://www.kaggle.com/code/everydaycodings/complete-analysis-on-fruits-and-vegetables-prices/input

# ETL(Extract)

Monthly temperatures


import pandas as pd
from sqlalchemy import create_engine

"""
AWS-loaded files removed for security reasons. Please access original data using links above.
"""

print(len(monthly_temp))
print(len(pollination_data))
print(len(honey_production))
print(len(product_price))

print(honey_production.isnull().sum())
print(monthly_temp.isnull().sum())
print(pollination_data.isnull().sum())
print(product_price.isnull().sum())

print(honey_production.head())
print(monthly_temp.head())
print(pollination_data.head())
print(product_price.head())

"""# ETL(Transform)

Monthly_temp
"""

print(monthly_temp)

print(monthly_temp.columns)

monthly_temp_cols_dropped = monthly_temp.copy(deep=True)

monthly_temp_cols_dropped.drop(monthly_temp_cols_dropped.columns[0], axis=1, inplace=True)

state_abbreviations = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY'
}

monthly_temp_cols_dropped['state'] = monthly_temp_cols_dropped['state'].replace(state_abbreviations)

# Combine month and year into a single column
monthly_temp_cols_dropped['month_year'] = pd.to_datetime(monthly_temp_cols_dropped[['year', 'month']].assign(day=1))

print(monthly_temp_cols_dropped.columns)

monthly_temp_cols_dropped

"""Pollination data"""

print(pollination_data.head)

# Check how many semicolons are in each row
pollination_data['semicolon_count'] = pollination_data['line;unicode;article_code;DOI_citation;species;crop_name;family;plant accession;crop part;continent;country;locality;"latitude;longitude";precision;year_of_the_experiment;scale;supplement_type;factors;FS_SUP_m;FS_SUP_sd;FS_SUP_n;FS_NAT_m;FS_NAT_sd;FS_NAT_n;SS_SUP_m;SS_SUP_sd;SS_SUP_n;SS_NAT_m;SS_NAT_sd;SS_NAT_n;SN_SUP_m;SN_SUP_sd;SN_SUP_n;SN_NAT_m;SN_NAT_sd;SN_NAT_n;FW_SUP_m;FW_SUP_sd;FW_SUP_n;FW_NAT_m;FW_NAT_sd;FW_NAT_n;SW_SUP_m;SW_SUP_sd;SW_SUP_n;SW_NAT_m;SW_NAT_sd;SW_NAT_n;FS_BAG_m;FS_BAG_sd;FS_BAG_n;SS_BAG_m;SS_BAG_sd;SS_BAG_n;SN_BAG_m;SN_BAG_sd;SN_BAG_n;FW_BAG_m;FW_BAG_sd;FW_BAG_n;SW_BAG_m;SW_BAG_sd;SW_BAG_n;PL_proportion;PL_effect_size;effect_size_constant'].str.count(';')
pollination_data[['line;unicode;article_code;DOI_citation;species;crop_name;family;plant accession;crop part;continent;country;locality;"latitude;longitude";precision;year_of_the_experiment;scale;supplement_type;factors;FS_SUP_m;FS_SUP_sd;FS_SUP_n;FS_NAT_m;FS_NAT_sd;FS_NAT_n;SS_SUP_m;SS_SUP_sd;SS_SUP_n;SS_NAT_m;SS_NAT_sd;SS_NAT_n;SN_SUP_m;SN_SUP_sd;SN_SUP_n;SN_NAT_m;SN_NAT_sd;SN_NAT_n;FW_SUP_m;FW_SUP_sd;FW_SUP_n;FW_NAT_m;FW_NAT_sd;FW_NAT_n;SW_SUP_m;SW_SUP_sd;SW_SUP_n;SW_NAT_m;SW_NAT_sd;SW_NAT_n;FS_BAG_m;FS_BAG_sd;FS_BAG_n;SS_BAG_m;SS_BAG_sd;SS_BAG_n;SN_BAG_m;SN_BAG_sd;SN_BAG_n;FW_BAG_m;FW_BAG_sd;FW_BAG_n;SW_BAG_m;SW_BAG_sd;SW_BAG_n;PL_proportion;PL_effect_size;effect_size_constant', 'semicolon_count']].head(10)

print(pollination_data.columns)

pollination_data_cols_dropped = pollination_data.copy(deep=True)

print(pollination_data_cols_dropped.shape)

print(pollination_data_cols_dropped is None)  # This should print 'False'
print(pollination_data_cols_dropped.columns)  # This should show the columns of the copied DataFrame

pollination_data_cols_dropped = pollination_data_cols_dropped.drop('semicolon_count', axis=1)

# Print out the exact representation of the column names
print([repr(col) for col in pollination_data_cols_dropped.columns])

print(pollination_data_cols_dropped.head)

pollination_data_cols_dropped = pollination_data_cols_dropped['line;unicode;article_code;DOI_citation;species;crop_name;family;plant accession;crop part;continent;country;locality;"latitude;longitude";precision;year_of_the_experiment;scale;supplement_type;factors;FS_SUP_m;FS_SUP_sd;FS_SUP_n;FS_NAT_m;FS_NAT_sd;FS_NAT_n;SS_SUP_m;SS_SUP_sd;SS_SUP_n;SS_NAT_m;SS_NAT_sd;SS_NAT_n;SN_SUP_m;SN_SUP_sd;SN_SUP_n;SN_NAT_m;SN_NAT_sd;SN_NAT_n;FW_SUP_m;FW_SUP_sd;FW_SUP_n;FW_NAT_m;FW_NAT_sd;FW_NAT_n;SW_SUP_m;SW_SUP_sd;SW_SUP_n;SW_NAT_m;SW_NAT_sd;SW_NAT_n;FS_BAG_m;FS_BAG_sd;FS_BAG_n;SS_BAG_m;SS_BAG_sd;SS_BAG_n;SN_BAG_m;SN_BAG_sd;SN_BAG_n;FW_BAG_m;FW_BAG_sd;FW_BAG_n;SW_BAG_m;SW_BAG_sd;SW_BAG_n;PL_proportion;PL_effect_size;effect_size_constant'].str.split(';', expand=True)

pollination_data_cols_dropped.columns

print([repr(col) for col in pollination_data_cols_dropped.columns])

# Drop the last four columns by index
pollination_data_cols_dropped.drop(pollination_data_cols_dropped.columns[-4:], axis=1, inplace=True)

# Define the correct column names
original_column_names = [
    'line', 'unicode', 'article_code', 'DOI_citation', 'species', 'crop_name', 'family',
    'plant accession', 'crop part', 'continent', 'country', 'locality', 'latitude',
    'longitude', 'precision', 'year_of_the_experiment', 'scale', 'supplement_type', 'factors',
    'FS_SUP_m', 'FS_SUP_sd', 'FS_SUP_n', 'FS_NAT_m', 'FS_NAT_sd', 'FS_NAT_n', 'SS_SUP_m',
    'SS_SUP_sd', 'SS_SUP_n', 'SS_NAT_m', 'SS_NAT_sd', 'SS_NAT_n', 'SN_SUP_m', 'SN_SUP_sd',
    'SN_SUP_n', 'SN_NAT_m', 'SN_NAT_sd', 'SN_NAT_n', 'FW_SUP_m', 'FW_SUP_sd', 'FW_SUP_n',
    'FW_NAT_m', 'FW_NAT_sd', 'FW_NAT_n', 'SW_SUP_m', 'SW_SUP_sd', 'SW_SUP_n', 'SW_NAT_m',
    'SW_NAT_sd', 'SW_NAT_n', 'FS_BAG_m', 'FS_BAG_sd', 'FS_BAG_n', 'SS_BAG_m', 'SS_BAG_sd',
    'SS_BAG_n', 'SN_BAG_m', 'SN_BAG_sd', 'SN_BAG_n', 'FW_BAG_m', 'FW_BAG_sd', 'FW_BAG_n',
    'SW_BAG_m', 'SW_BAG_sd', 'SW_BAG_n', 'PL_proportion', 'PL_effect_size', 'effect_size_constant'
]

# Rename the columns of the DataFrame
pollination_data_cols_dropped.columns = original_column_names

pollination_data_cols_dropped = pollination_data_cols_dropped.T.drop_duplicates().T

pollination_data_cols_dropped.columns

# Check if 'year_of_the_experiment' exists before creating the 'year' column
if 'year_of_the_experiment' in pollination_data_cols_dropped.columns:
    pollination_data_cols_dropped['year'] = pollination_data_cols_dropped['year_of_the_experiment']
    print("Year column successfully created.")
else:
    print("'year_of_the_experiment' column does not exist")

pollination_data_cols_dropped.columns

pollination_data_cols_dropped = pollination_data_cols_dropped.drop(['year_of_the_experiment'] ,axis='columns')

pollination_data_cols_dropped.columns

pollination_data_cols_dropped.shape

pollination_data_cols_dropped.head()

"""Honey production"""

honey_production

honey_production.columns

honey_production_cols_dropped = honey_production.copy(deep=True)

honey_production_cols_dropped

"""Product Price"""

product_price

product_price.columns

product_price_cols_dropped = product_price.copy(deep=True)

product_price_cols_dropped['date'] = pd.to_datetime(product_price_cols_dropped['date'])

# Step 2: Extract the year from the 'date' column
product_price_cols_dropped['year'] = product_price_cols_dropped['date'].dt.year

city_to_state = {
    'atlanta': 'GA',
    'chicago': 'IL',
    'losangeles': 'CA',
    'newyork': 'NY'
}

# Function to determine state based on available retail city
def get_state(row):
    if row['atlantaretail'] != '':
        return city_to_state['atlanta']
    elif row['chicagoretail'] != '':
        return city_to_state['chicago']
    elif row['losangelesretail'] != '':
        return city_to_state['losangeles']
    elif row['newyorkretail'] != '':
        return city_to_state['newyork']
    else:
        return None  # Or a default value

product_price_cols_dropped.columns

product_price_cols_dropped['date'] = pd.to_datetime(product_price_cols_dropped['date'])

# Extract month and year
product_price_cols_dropped['month'] = product_price_cols_dropped['date'].dt.month
product_price_cols_dropped['year'] = product_price_cols_dropped['date'].dt.year

# Convert columns to numeric by removing dollar signs and converting to float
price_columns = ['farmprice', 'atlantaretail', 'chicagoretail', 'losangelesretail', 'newyorkretail', 'averagespread']

for col in price_columns:
    # Remove dollar signs and convert to float, ignoring empty strings
    product_price_cols_dropped[col] = (
        product_price_cols_dropped[col]
        .replace({'\$': '', '%': ''}, regex=True)  # Remove dollar signs
    )

    # Convert to float, ignoring empty strings (they become NaN automatically)
    product_price_cols_dropped[col] = pd.to_numeric(product_price_cols_dropped[col], errors='coerce')

# Group by produce, year, and month and calculate averages
product_price_cols_dropped = product_price_cols_dropped.groupby(['productname', 'year', 'month']).agg({
    'farmprice': 'mean',
    'atlantaretail': 'mean',
    'chicagoretail': 'mean',
    'losangelesretail': 'mean',
    'newyorkretail': 'mean',
    'averagespread': 'mean'
}).reset_index()

# View the averaged DataFrame
product_price_cols_dropped

product_price_cols_dropped = product_price_cols_dropped.drop_duplicates(keep='first')

product_price_cols_dropped

product_price_cols_dropped.columns

"""Transform"""

print(monthly_temp_cols_dropped.columns)
print(product_price_cols_dropped.columns)
print(honey_production_cols_dropped.columns)
print(pollination_data_cols_dropped.columns)

print(monthly_temp_cols_dropped.head())
print(product_price_cols_dropped.head())
print(honey_production_cols_dropped.head())
print(pollination_data_cols_dropped.head())

"""Load data

Merge
"""

import pandas as pd
from sqlalchemy import create_engine

# Replace with your actual database connection string
engine = create_engine('postgre link: please reach out for use of code.')

monthly_temp_cols_dropped.to_sql('monthly_temp_cols_dropped', con=engine, if_exists='replace', index=False)
product_price_cols_dropped.to_sql('product_price_cols_dropped', con=engine, if_exists='replace', index=False)
honey_production_cols_dropped.to_sql('honey_production_cols_dropped', con=engine, if_exists='replace', index=False)
pollination_data_cols_dropped.to_sql('pollination_data_cols_dropped', con=engine, if_exists='replace', index=False)

from sqlalchemy import create_engine, text


query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';
"""

# Use `text()` for the query and execute it
with engine.connect() as connection:
    result = connection.execute(text(query))

    # Print the list of tables
    print("Tables in the RDS instance:")
    for row in result:
        print(row[0])

query = """
SELECT *
FROM product_price_cols_dropped
WHERE productname = 'Strawberries'
AND month = 5
AND year = 2019;
"""

# Step 4: Execute the query and fetch results into a DataFrame
with engine.connect() as connection:
    df_strawberries_may_2019 = pd.read_sql(text(query), connection)

# Display the DataFrame
df_strawberries_may_2019

# Define the SQL query for monthly temperature data for Texas in July
query_monthly_temp = """
SELECT *
FROM monthly_temp_cols_dropped
WHERE state = 'TX'
AND month = 7
AND year = 2019;
"""

# Execute the query and fetch results into a DataFrame
with engine.connect() as connection:
    df_monthly_temp_tx_july = pd.read_sql(text(query_monthly_temp), connection)

# Display the DataFrame
df_monthly_temp_tx_july

# Define the SQL query for honey production in California in the year 2018
query_honey_production = """
SELECT *
FROM honey_production_cols_dropped
WHERE state = 'CA'
AND year = 2003;
"""

# Execute the query and fetch results into a DataFrame
with engine.connect() as connection:
    df_honey_production_ca_2018 = pd.read_sql(text(query_honey_production), connection)

# Display the DataFrame
df_honey_production_ca_2018

# Define the SQL query for joining product_price and monthly_temp tables
query_join_specific = """
SELECT
    pp.productname,
    pp.farmprice,
    pp.atlantaretail,
    pp.chicagoretail,
    pp.losangelesretail,
    pp.newyorkretail,
    pp.averagespread,
    mt.average_temp,
    mt.monthly_mean_from_1901_to_2000,
    mt.centroid_lon,
    mt.centroid_lat,
    CONCAT(mt.month, '-', mt.year) AS month_year
FROM product_price_cols_dropped AS pp
JOIN monthly_temp_cols_dropped AS mt
ON pp.month = mt.month
AND pp.year = mt.year
WHERE pp.month = 5
AND pp.year = 2019;
"""

# Execute the query and fetch results into a DataFrame
with engine.connect() as connection:
    df_joined_data = pd.read_sql(text(query_join_specific), connection)

# Display the DataFrame
df_joined_data

query_join_pollination_honeyproduction = """
SELECT *
FROM pollination_data_cols_dropped AS pd
JOIN honey_production_cols_dropped AS hp
ON CAST(pd.year AS INT) = CAST(hp.year AS INT)
WHERE pd.year ~ '^[0-9]+$'
AND CAST(pd.year AS INT) = 2003;
"""

# Execute the query and fetch results into a DataFrame
with engine.connect() as connection:
    df_joined_data = pd.read_sql(text(query_join_pollination_honeyproduction), connection)

# Display the DataFrame
print(df_joined_data)

query_join_honey_temp = """
SELECT
    hp.state,
    hp.numcol,
    hp.yieldpercol,
    hp.totalprod,
    hp.priceperlb,
    hp.prodvalue,
    mt.average_temp,
    mt.monthly_mean_from_1901_to_2000,
    mt.centroid_lon,
    mt.centroid_lat,
    mt.month_year
FROM honey_production_cols_dropped AS hp
JOIN monthly_temp_cols_dropped AS mt
ON CAST(hp.year AS INT) = CAST(mt.year AS INT)
WHERE CAST(hp.year AS INT) = 2007;  -- Filter for the specific year
"""

# Execute the query and fetch results into a DataFrame
with engine.connect() as connection:
    df_joined_data = pd.read_sql(text(query_join_honey_temp), connection)

# Display the DataFrame
print(df_joined_data)
