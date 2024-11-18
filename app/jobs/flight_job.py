from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from jobs.functions.functions import (
    create_raw_pyspark_datarame,
    fix_airports_data_df,
    num_of_bookings,
    calculate_revenue,
    calculate_airtime,
)
from jobs.functions.functions import (
    num_of_flights_day_period,
    flight_delay,
    occupancy,
    occupancy_fare_class,
)


def table_load(spark, table_name):
    """
    Function to load particular table as PySpark Dataframe

    Args:
        spark: SparkSession built in main.py.
        table_name: Table name you want to load.

    Returns:
        Pyspark Dataframe extracted from Google BigQuery depending on given table_name.
    """
    return (
        spark.read.format("bigquery")
        .option("table", f"mentoring-372319.flights_database.{table_name}")
        .load()
    )


def get_aggregates(
    spark_df_aircrafts_data,
    spark_df_airports_data,
    spark_df_bookings,
    spark_df_flights,
    spark_df_seats,
    spark_df_ticket_flights,
    spark_df_tickets,
):
    """
    Function calculates various aggregates such as:
    - Daily number of bookings per airport;
    - Daily revenue per airport;
    - Daily airtime per airport;
    - Daily flights per airport per day period;
    - Daily flight delay per airport;
    - Daily flight occupancy per airport;
    - Daily flight occupancy per fare class per airport;

    Args:
        spark_df_aircrafts_data: Extracted table of aircrafts from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.
        spark_df_bookings: Extracted table of bookings from Bigquery converted to Pyspark Dataframe.
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_seats: Extracted table of seats from Bigquery converted to Pyspark Dataframe.
        spark_df_ticket_flights: Extracted table of flight tickets from Bigquery converted to Pyspark Dataframe.
        spark_df_tickets: Extracted table of tickets from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing all aggregates.
    """

    booking_aggregate_df = num_of_bookings(
        spark_df_bookings,
        spark_df_tickets,
        spark_df_ticket_flights,
        spark_df_flights,
        spark_df_airports_data,
    )

    revenue_aggregate_df = calculate_revenue(
        spark_df_flights, spark_df_airports_data, spark_df_ticket_flights
    )

    airtime_aggregate_df = calculate_airtime(spark_df_flights, spark_df_airports_data)

    flights_day_period_aggregate_df = num_of_flights_day_period(
        spark_df_flights, spark_df_airports_data
    )

    delay_aggregate_df = flight_delay(spark_df_flights, spark_df_airports_data)

    occupancy_aggregate_df = occupancy(
        spark_df_flights,
        spark_df_seats,
        spark_df_ticket_flights,
        spark_df_airports_data,
    )

    occupancy_business_fare_aggregate_df = occupancy_fare_class(
        spark_df_seats,
        spark_df_aircrafts_data,
        spark_df_ticket_flights,
        spark_df_flights,
        spark_df_airports_data,
    )

    initial_df = create_raw_pyspark_datarame(spark_df_airports_data, spark_df_flights, spark_df_bookings)

    final_df = (
        initial_df.join(
            booking_aggregate_df,
            (initial_df["date"] == booking_aggregate_df["book_date"])
            & (initial_df["airport_name"] == booking_aggregate_df["airport_name"]),
            "left",
        )
        .drop(booking_aggregate_df.book_date)
        .drop(booking_aggregate_df.airport_name)
    )

    final_df = (
        final_df.join(
            revenue_aggregate_df,
            (final_df["date"] == revenue_aggregate_df["date"])
            & (final_df["airport_name"] == revenue_aggregate_df["airport_name"]),
            "left",
        )
        .drop(revenue_aggregate_df.date)
        .drop(revenue_aggregate_df.airport_name)
    )

    final_df = (
        final_df.join(
            airtime_aggregate_df,
            (final_df["date"] == airtime_aggregate_df["date"])
            & (final_df["airport_name"] == airtime_aggregate_df["airport_name"]),
            "left",
        )
        .drop(airtime_aggregate_df.date)
        .drop(airtime_aggregate_df.airport_name)
    )

    final_df = (
        final_df.join(
            flights_day_period_aggregate_df,
            (final_df["date"] == flights_day_period_aggregate_df["date"])
            & (
                final_df["airport_name"]
                == flights_day_period_aggregate_df["airport_name"]
            ),
            "left",
        )
        .drop(flights_day_period_aggregate_df.date)
        .drop(flights_day_period_aggregate_df.airport_name)
    )

    final_df = (
        final_df.join(
            delay_aggregate_df,
            (final_df["date"] == delay_aggregate_df["date"])
            & (final_df["airport_name"] == delay_aggregate_df["airport_name"]),
            "left",
        )
        .drop(delay_aggregate_df.date)
        .drop(delay_aggregate_df.airport_name)
    )

    final_df = (
        final_df.join(
            occupancy_aggregate_df,
            (final_df["date"] == occupancy_aggregate_df["date"])
            & (final_df["airport_name"] == occupancy_aggregate_df["airport_name"]),
            "left",
        )
        .drop(occupancy_aggregate_df.date)
        .drop(occupancy_aggregate_df.airport_name)
    )

    final_df = final_df.withColumnRenamed('airport_name', 'final_airport_name')

    final_df = (
        final_df.join(
            occupancy_business_fare_aggregate_df,
            (final_df["date"] == occupancy_business_fare_aggregate_df["date"])
            & (
                final_df["final_airport_name"]
                == occupancy_business_fare_aggregate_df["airport_name"]
            ),"left",
                    )
        .drop(occupancy_business_fare_aggregate_df.date)
        .drop(occupancy_business_fare_aggregate_df.airport_name)
    )



    final_df = final_df.select(
        "date",
        "final_airport_name",
        "bookings",
        "revenue",
        "airtime_minutes",
        "flights_early_morning",
        "flights_morning",
        "flights_afternoon",
        "flights_evening",
        "avg_delay_seconds",
        "avg_occupancy_percent",
        "avg_occupancy_economy",
        "avg_occupancy_business",
        "avg_occupancy_comfort"
    )
    
    final_df = final_df.orderBy(col("date").asc(), col("final_airport_name").asc())
    final_df = final_df.withColumnRenamed('final_airport_name', 'airport_name')

    final_df = final_df.fillna(0)

    return final_df


def data_upload(final_df):
    """
    Uploaded aggregate PySpark Dataframe to Google Bigquery table.

    Args:
        final_df: PySpark Dataframe containing all aggregates.

    Returns:
        None
    """

    final_df.write \
        .format('bigquery') \
        .option('table', 'mentoring-372319.flights_database.aggregates_pk') \
        .option("temporaryGcsBucket", "flight-bucket-new") \
        .mode("overwrite") \
    .save()


def run_job(spark):
    """
    Running ETL job to calculate and upload aggregates.

    Args:
        spark: SparkSession built in main.py.

    Returns:
        None
    """
    aircrafts = table_load(spark, "aircrafts_data")
    airports = table_load(spark, "airports_data")
    bookings = table_load(spark, "bookings")
    flights = table_load(spark, "flights")
    seats = table_load(spark, "seats")
    tickets_flights = table_load(spark, "ticket_flights_v1")
    tickets = table_load(spark, "tickets")
    
    aggregated_data = get_aggregates(    
        aircrafts, airports, bookings, 
        flights, seats, tickets_flights, tickets
    )
    
    data_upload(aggregated_data)
