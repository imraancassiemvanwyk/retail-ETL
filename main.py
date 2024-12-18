import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def extract_data(file_path):
    data = pd.read_csv(file_path)
    return data

def Tranform(gender,data):
    table1 = data[data['Gender'] == 'Male']
    table2 = data[data['Gender'] == 'Female']
    if gender == 'Male':
        return table1
    if gender == 'Female':
        return table2

def Load_data(data):

    db_connection_string = 'postgresql://postgres:postgres@127.0.0.1:5433/male users'
    engine = create_engine(db_connection_string)
    data2 = Tranform('Male',data)
    data.to_sql('male_users', engine, if_exists='append', index=False)
    data3 = Tranform('Female',data)
    data.to_sql('male_users', engine, if_exists='append', index=False)

    return 0


print(Load_data(extract_data('retail_sales_dataset.csv')))