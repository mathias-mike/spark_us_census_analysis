import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Custom ustil functions
from utils.columns_enum import ValidColumns
from utils import data_dictionary


def create_spark_session():
    '''
    Get an existing spark session or Create new one if not exist.

    Return
    ---
    spark (SparkSession) - spark session connected to local mode deployment of spark
    '''
    spark = SparkSession \
        .builder \
        .appName("Testing Spark") \
        .getOrCreate()
    return spark


def extract_columns(spark, input_data, output_data):
    '''
    Loads United States Census Bureau's 2017 Basic Monthly CPS data from disk

    Parameters
    ----
    spark: SparkSession
        Used to perform operations
    input_data: str
        File path of input data location
    output_data: str
        File path of output data location
    '''
    df = spark.read.json(input_data)

    # Extracting Full household identifier.
    extract_column = udf(lambda row: row[0:15] + row[70:75], StringType())
    df = df.withColumn(ValidColumns.FULL_HOUSEHOLD_IDENTIFIER.value, extract_column("value"))

    # Extracting Time of interview in YYYY/MMM format.
    extract_column = udf(lambda row: row[17:21] + '/' + data_dictionary.MONTHS.get(int(row[15:17])), StringType())
    df = df.withColumn(ValidColumns.TIME_OF_INTERVIEW.value, extract_column("value"))

    # Extracting Final outcome of the survey.
    extract_column = udf(lambda row: data_dictionary.VALID_FINAL_OUTCOME.get(row[23:26].zfill(3)), StringType())
    df = df.withColumn(ValidColumns.FINAL_OUTCOME_OF_SURVEY.value, extract_column("value"))

    # Extracting Type of housing unit.
    extract_column = udf(lambda row: data_dictionary.VALID_TYPE_OF_HOUSING_UNIT.get(int(row[30:32])), StringType())
    df = df.withColumn(ValidColumns.TYPE_OF_HOUSING_UNIT.value, extract_column("value"))

    # Extracting Household type.
    extract_column = udf(lambda row: data_dictionary.VALID_HOUSEHOLD_TYPE.get(int(row[60:62])), StringType())
    df = df.withColumn(ValidColumns.HOUSEHOLD_TYPE.value, extract_column("value"))

    # Extracting Apartment/Household has a telephone.
    extract_column = udf(lambda row: data_dictionary.VALID_YES_NO.get(int(row[32:34])), StringType())
    df = df.withColumn(ValidColumns.HOUSEHOLD_HAS_TELEPHONE.value, extract_column("value"))

    # Extracting Apartment/Household can access a telephone elsewhere.
    extract_column = udf(lambda row: data_dictionary.VALID_YES_NO.get(int(row[34:36])), StringType())
    df = df.withColumn(ValidColumns.HOUSEHOLD_CAN_ACCESS_TELEPHONE_ELSEWHERE.value, extract_column("value"))

    # Extracting Is telephone interview acceptable for the responder.
    extract_column = udf(lambda row: data_dictionary.VALID_YES_NO.get(int(row[36:38])), StringType())
    df = df.withColumn(ValidColumns.IS_TELEPHONE_INTERVIEW_ACCEPTABLE_FOR_THE_RESPONDER.value, extract_column("value"))

    # Extracting Type of interview.
    extract_column = udf(lambda row: data_dictionary.VALID_TYPE_OF_INTERVIEW.get(int(row[64:66])), StringType())
    df = df.withColumn(ValidColumns.TYPE_OF_INTERVIEW.value, extract_column("value"))

    # Extracting Family income range.
    extract_column = udf(lambda row: data_dictionary.VALID_FAMILY_INCOME_RANGE.get(int(row[38:40])), StringType())
    df = df.withColumn(ValidColumns.FAMILY_INCOME.value, extract_column("value"))

    # Extracting Geographical division/location.
    extract_column = udf(lambda row: data_dictionary.VALID_DIVISIONS.get(int(row[90:91])) + '/' + data_dictionary.VALID_REGIONS.get(int(row[88:90])), StringType())
    df = df.withColumn(ValidColumns.GEOGRAPHICAL_DIVISION_LOCATION.value, extract_column("value"))

    # Extracting Race
    extract_column = udf(lambda row: data_dictionary.VALID_RACE.get(row[138:140].zfill(2)), StringType())
    df = df.withColumn(ValidColumns.RACE.value, extract_column("value"))


    
    # Export
    column_list = [col.value for col in ValidColumns]
    df.select(column_list).write.csv("dec17pub.csv", mode="overwrite") # Depending on how you want the data to be writtten out, cvs just for dummy work


def main():
    spark = create_spark_session()
    input_data = "/home/ubuntu/dec17pub.dat"
    output_data = "<output location>"
    
    extract_columns(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()