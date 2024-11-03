from pyspark.sql import SparkSession
from jobs import flight_job

def main():
    """
    Creating Spark Session with connection to Bigquery.
    """
    spark = SparkSession.builder \
    .appName('Airport_aggregate_job') \
    .getOrCreate()
    flight_job.run_job(spark)

if __name__ == "__main__":
    main()