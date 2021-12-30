from google.cloud import bigquery
import psycopg2
from config import pgsql_config
import sql

if __name__ == '__main__':
    client = bigquery.Client()
    query = client.query(
        """
        with cte as(
            select geo_id,sub_region_1,sub_region_2,retail_and_recreation_percent_change_from_baseline,median_age,median_rent from `bigquery-public-data.census_bureau_acs.county_2017_1yr`
            join `bigquery-public-data.covid19_google_mobility.mobility_report`
            on census_fips_code = geo_id ||".0"
            where median_rent<2000 and median_age <30)
            
        select geo_id,sub_region_1,sub_region_2, avg(retail_and_recreation_percent_change_from_baseline) sales_vector from cte
        group by sub_region_2, sub_region_1,geo_id
        having sales_vector > -15;
        """
    )
    county_list = []
    for row in query.result():
        county_list.append(row[0:4])
    print(len(county_list))

    connection = psycopg2.connect(f"""
            host='{pgsql_config['host']}'
            dbname='{pgsql_config['dbname']}'
            user='{pgsql_config['user']}'
            password='{pgsql_config['password']}'
        """)

    cursor = connection.cursor()
    connection.autocommit = True
    cursor.execute(sql.create_schema)
    print("Created schema")
    cursor.execute(sql.create_table1)
    print("Created table")
    for i in range(len(county_list)):
        cursor.execute(sql.insert_table1,county_list[i])
    connection.close()
