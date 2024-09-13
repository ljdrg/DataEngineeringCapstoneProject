import configparser
import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date, monotonically_increasing_id

import boto3


# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read('conf.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
#SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
OUTPUT_S3_BUCKET = config['S3']['OUTPUT_S3_BUCKET']

session = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], \
                        region_name="us-east-1")
s3 = session.resource("s3")
output_bucket = s3.Bucket(OUTPUT_S3_BUCKET)


# data processing functions
def create_spark_session():
    """Creates a spark session.
    """
    spark = SparkSession.builder.config("spark.jars.repositories", "https://repos.spark-packages.org/").\
            config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
            enableHiveSupport().getOrCreate()
    return spark


def transform_datetime(date):
    """Transforms SAS date into pandas timestamp.
    """
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
transform_datetime_udf = udf(transform_datetime, DateType())


def rename_columns(table, new_columns):
    """Renames the columns of the spark dfs with new column names. 
        new columns: list of column names
    """
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def process_immigration(spark, output_data):
    """Process immigration data to get fact_im, dim_im_person and dim_im_airline.
    """

    logging.info("Start loading immigration data")
    

    # read immigration data file
    # data_im = os.path.join(input_data + 'immigration/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    # df = spark.read.format('com.github.saurfang.sas.spark').load(data_im)
    
    df = spark.read.format("com.github.saurfang.sas.spark").load("../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat")
    

    logging.info("Start processing fact_im")
    # create fact_im table
    fact_im = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr','arrdate', 'depdate', 'i94mode', 'i94visa').distinct()
    new_columns = ['immigration_id', 'year', 'month', 'city_code', 'state_code',\
                   'arrival_date', 'departure_date', 'transportation_mode', 'visa']
    fact_im = rename_columns(fact_im, new_columns)
    fact_im = fact_im.withColumn('country', lit('United States'))
    fact_im = fact_im.withColumn('arrival_date', transform_datetime_udf(col('arrival_date')))
    fact_im = fact_im.withColumn('departure_date', transform_datetime_udf(col('departure_date')))
    # write fact_im table to parquet files partitioned by state
    output_path = os.path.join(output_data, 'fact_im')
    fact_im.write.mode("overwrite").partitionBy('state_code').parquet(path=output_path)

    
    logging.info("Start processing dim_im_person")
    # create dim_im_person table
    dim_im_person = df.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender').distinct()
    new_columns = ['immigration_id', 'citizen_country', 'residence_country', 'birth_year', 'gender']
    dim_im_person = rename_columns(dim_im_person, new_columns)
    # write dim_im_person table to parquet files
    output_path = os.path.join(output_data, 'dim_im_person')
    #dim_im_person.write.mode("overwrite").parquet(path=output_path)
    
    logging.info("Start processing dim_im_airline")
    # create dim_im_airline table
    dim_im_airline = df.select('cicid', 'airline', 'fltno', 'visatype').distinct()
    new_columns = ['immigration_id', 'airline', 'flight_number', 'visa_type']
    dim_im_airline = rename_columns(dim_im_airline, new_columns)
    # write dim_im_airline table to parquet files
    output_path = os.path.join(output_data, 'dim_im_airline')
    #dim_im_airline.write.mode("overwrite").parquet(path=output_path)
    



def process_temperature(spark, output_data):
    """ Process temperature data to get dim_temp.
    """

    logging.info("Start processing dim_temp")
    # read temperature data 
    # temp_data = os.path.join(input_data + 'temperature/GlobalLandTemperaturesByCity.csv')
    df = spark.read.option("header", True).csv('../../data2/GlobalLandTemperaturesByCity.csv')

    df = df.where(df['Country'] == 'United States')
    dim_temp = df.select(['dt', 'AverageTemperature', 'AverageTemperatureUncertainty',\
                         'City', 'Country', 'Latitude', 'Longitude']).distinct()

    new_columns = ['date', 'avg_temperature', 'avg_temp_uncertainty', 'City', 'Country', 'Latitude', 'Longitude']
    dim_temp = rename_columns(dim_temp, new_columns)

    dim_temp = dim_temp.withColumn('date', to_date(col('date')))
    dim_temp = dim_temp.withColumn('year', year(dim_temp['date']))
    dim_temp = dim_temp.withColumn('month', month(dim_temp['date']))
    dim_temp = dim_temp.withColumn('City', upper(col('City')))
 
    # write dim_temp table to parquet files
    # dim_temp.write.mode("overwrite").parquet(path=output_data + 'dim_temp')
    output_path = os.path.join(output_data, 'dim_temp')
    dim_temp.write.mode("overwrite").parquet(path=output_path)


def process_demography(spark, output_data):
    """ Process demograpy data to get dim_demog_state.
    """

    logging.info("Start processing dim_demog_state")
    # read demography data 
    #demog_data = os.path.join(input_data + 'demography/us-cities-demographics.csv')
    df = spark.read.option("header", True).options(delimiter=';').csv("us-cities-demographics.csv")


    dim_demog_state = df.select(['State Code', 'State', 'Male Population', 'Female Population', \
                              'Number of Veterans', 'Foreign-born', 'Median Age', 'Average Household Size']).distinct() \
                              .withColumn("demog_state_id", monotonically_increasing_id())
    new_columns = ['city', 'state', 'male_population', 'female_population', \
                   'num_vetarans', 'foreign_born', 'median_age', 'avg_household_size']
    dim_demog_state = rename_columns(dim_demog_state, new_columns)

    # write dim_demog_population table to parquet files
    # dim_demog_state.write.mode("overwrite").parquet(path=output_data + 'dim_demog_state')
    output_path = os.path.join(output_data, 'dim_demog_state')
    dim_demog_state.write.mode("overwrite").parquet(path=output_path)

    

def process_labels_im(spark, output_data):
    """ Get codes of country, city and state from labels description.
    """

    logging.info("Start processing labels")
    labels = "I94_SAS_Labels_Descriptions.SAS"
    with open(labels) as f:
        contents = f.readlines()

    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
    output_path = os.path.join(output_data, 'country_code')
    spark.createDataFrame(country_code.items(), ['code', 'country']).write.mode("overwrite")\
         .parquet(path=output_path)

    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"),pair[1].strip('\t').strip().strip("''")
        city_code[code] = city
    output_path = os.path.join(output_data, 'city_code')
    spark.createDataFrame(city_code.items(), ['code', 'city']).write.mode("overwrite")\
         .parquet(path=output_path)

    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state
    output_path = os.path.join(output_data, 'state_code')
    spark.createDataFrame(state_code.items(), ['code', 'state']).write.mode("overwrite")\
         .parquet(path=output_path)
    
def main():
    spark = create_spark_session()
    output_data = "output_data" #OUTPUT_S3_BUCKET
    
    process_immigration(spark, output_data)    
    process_labels_im(spark, output_data)
    process_temperature(spark, output_data)
    process_demography(spark, output_data)
    logging.info("Data processing completed")


if __name__ == "__main__":
    main()