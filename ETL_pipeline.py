import datetime as dt

import pandas as pd
import psycopg2

filesToExtract = ['sales_records_n1.csv', 'sales_records_n2.csv', 'sales_records_n3.csv', 'sales_records_n4.csv', 'sales_records_n5.csv']

def transform_date(date):
    # Transform date from MM/DD/YYYY to YYYY-MM-DD
    return dt.datetime.strptime(date, '%m/%d/%Y').strftime('%Y/%m/%d')


def transform_money(money):
    #Making sure that money is a float with 1 decimal places
    return round(float(money),1)

def extract_transform_files( filesToExtract ):
    #Extract and transform data from files
    transformedFile = []
    for file in filesToExtract:
        data = pd.read_csv(file)
        data['Order Date'] = data['Order Date'].apply(transform_date)
        data['Ship Date'] = data['Ship Date'].apply(transform_date)
        data['Unit Price'] = data['Unit Price'].apply(transform_money)
        data['Unit Cost'] = data['Unit Cost'].apply(transform_money)
        data['Total Revenue'] = data['Total Revenue'].apply(transform_money)
        data['Total Cost'] = data['Total Cost'].apply(transform_money)
        data['Total Profit'] = data['Total Profit'].apply(transform_money)
        transformedFile.append(data)
    return transformedFile

def load(dataRows):
    # Load data into database
    conn = psycopg2.connect("dbname=etl_db user=postgres password=adnane2000")
    cursor = conn.cursor()
    for data in dataRows:
        for _, row in data.iterrows():
            cursor.execute(
                """
                INSERT INTO sales(region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit)   
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}) ON CONFLICT (order_id) DO NOTHING;
                """.format(row['Region'], row['Country'].replace("'", "-"), row['Item Type'], row['Sales Channel'], row['Order Priority'],
                           row['Order Date'], row['Order ID'], row['Ship Date'], row['Units Sold'], row['Unit Price'],
                           row['Unit Cost'], row['Total Revenue'], row['Total Cost'], row['Total Profit'])
            )
    conn.commit()
    cursor.close()
    conn.close()
        
        

def etl_pipeline(fileToExtract):
    data = extract_transform_files(fileToExtract)
    load(data)
    print('ETL pipeline has been executed successfully')

etl_pipeline(filesToExtract)
