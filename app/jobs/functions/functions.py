import pyspark
import pyspark.sql.functions as func

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.functions import (
    avg as _avg,
    col,
    date_format,
    expr,
    hour,
    regexp_extract,
    round as _round,
    sum as _sum,
    unix_timestamp,
    when,
)

def fix_airports_data_df(spark_df_airports_data):
    """
    Extracting english names for city and airport.

    Args:
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Fixed Airports PySpark Dataframe.
    """
    regex_pattern_city = r"^([A-Za-z\.\-]+(?:\s[A-Za-z\.\-]+)?)"
    regex_pattern_airport = (
        r"^([A-Za-z\u0400-\u04FF\.\-]+(?:\s[A-Za-z\u0400-\u04FF\.\-]+)*)"
    )

    spark_df_airports_data = spark_df_airports_data.withColumn(
        "airport_name_map", expr("from_json(airport_name, 'MAP<STRING, STRING>')")
    )
    spark_df_airports_data = spark_df_airports_data.withColumn(
        "city_map", expr("from_json(city, 'MAP<STRING, STRING>')")
    )

    spark_df_airports_data = spark_df_airports_data.withColumn(
        "airport_name_final",
        regexp_extract(
            expr("concat_ws(' | ', map_values(airport_name_map))"),
            regex_pattern_airport,
            1,
        ),
    )
    spark_df_airports_data = spark_df_airports_data.withColumn(
        "city_name_final",
        regexp_extract(
            expr("concat_ws(' | ', map_values(city_map))"), regex_pattern_city, 1
        ),
    )

    spark_df_airports_data = spark_df_airports_data.drop(
        "airport_name", "city", "airport_name_map", "city_map"
    )
    spark_df_airports_data = spark_df_airports_data.withColumnRenamed(
        "airport_name_final", "airport_name"
    ).withColumnRenamed("city_name_final", "city_name")

    return spark_df_airports_data


def create_raw_pyspark_datarame(spark_df_airports_data, spark_df_flights, spark_df_bookings):
    """
    Creating PySpark Dataframe to get all possible combinations of Date & Airport by performing a Cartesian join.
    This will be original PySpark Dataframe to which later our job will append calculated aggregates.

    Args:
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_bookings: Extracted table of bookings from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing dates and airports made for joining aggregates.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    unique_airports_df = spark_df_airports_data.select("airport_name").distinct()
    dates_flights_df = spark_df_flights.withColumn(
        "scheduled_departure", date_format("scheduled_departure", "yyyy-MM-dd")
    )
    dates_bookings_df = spark_df_bookings.withColumn(
        "book_date", date_format("book_date", "yyyy-MM-dd")
    )

    dates_flights_df = dates_flights_df.select("scheduled_departure").distinct().withColumnRenamed("scheduled_departure", "date")
    dates_bookings_df = dates_bookings_df.select("book_date").distinct().withColumnRenamed("book_date", "date")    
    
    dates_df = dates_flights_df.union(dates_bookings_df).distinct()

    raw_df = dates_df.crossJoin(unique_airports_df)
    return raw_df


def num_of_bookings(
    spark_df_bookings,
    spark_df_tickets,
    spark_df_ticket_flights,
    spark_df_flights,
    spark_df_airports_data,
):
    """
    Creating aggregate of daily number of bookings per airport.

    Args:
        spark_df_bookings: Extracted table of bookings from Bigquery converted to Pyspark Dataframe.
        spark_df_tickets: Extracted table of tickets from Bigquery converted to Pyspark Dataframe.
        spark_df_ticket_flights: Extracted table of flight tickets from Bigquery converted to Pyspark Dataframe.
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily number of bookings per airport.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    spark_df_bookings = spark_df_bookings.join(
        spark_df_tickets,
        spark_df_bookings["book_ref"] == spark_df_tickets["book_ref"],
        "left",
    )
    spark_df_bookings = spark_df_bookings.join(
        spark_df_ticket_flights,
        spark_df_bookings["ticket_no"] == spark_df_ticket_flights["ticket_no"],
        "left",
    )
    spark_df_bookings = spark_df_bookings.join(
        spark_df_flights,
        spark_df_bookings["flight_id"] == spark_df_flights["flight_id"],
        "left",
    )
    spark_df_bookings = spark_df_bookings.join(
        spark_df_airports_data,
        spark_df_bookings["departure_airport"]
        == spark_df_airports_data["airport_code"],
        "left",
    )

    spark_df_bookings = spark_df_bookings.select("book_date", "airport_name")
    spark_df_bookings = spark_df_bookings.withColumn(
        "book_date", date_format("book_date", "yyyy-MM-dd")
    )
    spark_df_bookings = (
        spark_df_bookings.groupBy("book_date", "airport_name")
        .count()
        .withColumnRenamed("count", "bookings")
    )

    return spark_df_bookings


def calculate_revenue(spark_df_flights, spark_df_airports_data, spark_df_ticket_flights):
    """
    Creating aggregate of daily revenue per airport.

    Args:
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.
        spark_df_ticket_flights: Extracted table of flight tickets from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily revenue per airport.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    spark_df_flights = spark_df_flights.na.drop(subset=["flight_id"])
    spark_df_flights = spark_df_flights.join(
        spark_df_airports_data,
        spark_df_flights["departure_airport"] == spark_df_airports_data["airport_code"],
        "left",
    )
    spark_df_flights = spark_df_flights.withColumn(
        "scheduled_departure", date_format("scheduled_departure", "yyyy-MM-dd")
    )
    spark_df_flights = spark_df_flights.na.drop(
        subset=["actual_departure", "actual_arrival"]
    )
    spark_df_flights = spark_df_flights.filter(col("status") == "Arrived")

    revenue_per_flight = spark_df_ticket_flights.groupBy("flight_id").agg(
        _sum("amount").alias("flight_revenue")
    )

    spark_df_flights = spark_df_flights.join(
        revenue_per_flight,
        spark_df_flights["flight_id"] == revenue_per_flight["flight_id"],
        "left",
    )
    spark_df_flights = spark_df_flights.select(
        "scheduled_departure", "airport_name", "flight_revenue"
    )
    spark_df_flights = (
        spark_df_flights.groupBy("scheduled_departure", "airport_name")
        .agg(_sum("flight_revenue").alias("revenue"))
        .withColumnRenamed("scheduled_departure", "date")
    )

    spark_df_flights = spark_df_flights.withColumn("revenue", spark_df_flights["revenue"].cast(LongType()))

    return spark_df_flights


def calculate_airtime(spark_df_flights, spark_df_airports_data):
    """
    Creating aggregate of daily airtime per airport.

    Args:
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily airtime per airport.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    spark_df_flights = spark_df_flights.na.drop(subset=["flight_id"])
    spark_df_flights = spark_df_flights.join(
        spark_df_airports_data,
        spark_df_flights["departure_airport"] == spark_df_airports_data["airport_code"],
        "left",
    )
    spark_df_flights = spark_df_flights.withColumn(
        "scheduled_departure", date_format("scheduled_departure", "yyyy-MM-dd")
    ).withColumnRenamed("scheduled_departure", "date")
    spark_df_flights = spark_df_flights.na.drop(
        subset=["actual_departure", "actual_arrival"]
    )
    spark_df_flights = spark_df_flights.filter(col("status") == "Arrived")
    spark_df_flights = spark_df_flights.withColumn(
        "airtime_seconds",
        unix_timestamp(col("actual_arrival")) - unix_timestamp(col("actual_departure")),
    )
    spark_df_flights = spark_df_flights.groupBy("date", "airport_name").agg(
        _sum("airtime_seconds").alias("airtime_seconds")
    )
    spark_df_flights = spark_df_flights.withColumn(
        "airtime_minutes", _round((col("airtime_seconds") / 60), 2)
    )
    spark_df_flights = spark_df_flights.drop("airtime_seconds")

    return spark_df_flights


def num_of_flights_day_period(spark_df_flights, spark_df_airports_data):
    """
    Creating aggregate of daily flights per airport per day period.

    Args:
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily flights per airport per day period.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    spark_df_flights = spark_df_flights.na.drop(subset=["flight_id"])
    spark_df_flights = spark_df_flights.join(
        spark_df_airports_data,
        spark_df_flights["departure_airport"] == spark_df_airports_data["airport_code"],
        "left",
    )
    spark_df_flights = spark_df_flights.na.drop(
        subset=["actual_departure", "actual_arrival"]
    )
    spark_df_flights = spark_df_flights.filter(col("status") == "Arrived")

    spark_df_flights = spark_df_flights.withColumn(
        "date_hour", col("scheduled_departure").cast("timestamp")
    )
    spark_df_flights = spark_df_flights.withColumn(
        "scheduled_departure", date_format("scheduled_departure", "yyyy-MM-dd")
    ).withColumnRenamed("scheduled_departure", "date")
    spark_df_flights = spark_df_flights.withColumn("hour", hour(col("date_hour")))
    spark_df_flights = spark_df_flights.withColumn(
        "day_period",
        when((col("hour") >= 0) & (col("hour") < 6), "Early Morning")
        .when((col("hour") >= 6) & (col("hour") < 12), "Morning")
        .when((col("hour") >= 12) & (col("hour") < 18), "Afternoon")
        .when((col("hour") >= 18) & (col("hour") < 24), "Evening"),
    )

    spark_df_flights = spark_df_flights.groupBy(
        "date", "airport_name", "day_period"
    ).count()
    spark_df_flights = (
        spark_df_flights.groupBy("date", "airport_name")
        .pivot("day_period", ["Early Morning", "Morning", "Afternoon", "Evening"])
        .sum("count")
    )
    spark_df_flights = (
        spark_df_flights.fillna(0)
        .withColumnRenamed("Early Morning", "flights_early_morning")
        .withColumnRenamed("Morning", "flights_morning")
        .withColumnRenamed("Afternoon", "flights_afternoon")
        .withColumnRenamed("Evening", "flights_evening")
    )
    spark_df_flights = spark_df_flights.select(
        "date",
        "airport_name",
        "flights_early_morning",
        "flights_morning",
        "flights_afternoon",
        "flights_evening",
    )

    return spark_df_flights


def flight_delay(spark_df_flights, spark_df_airports_data):
    """
    Creating aggregate of daily flight delay per airport.

    Args:
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily flight delay per airport.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    spark_df_flights = spark_df_flights.join(
        spark_df_airports_data,
        spark_df_flights["departure_airport"] == spark_df_airports_data["airport_code"],
        "left",
    )

    spark_df_flights = spark_df_flights.na.drop(
        subset=["actual_departure", "actual_arrival"]
    )
    spark_df_flights = spark_df_flights.select(
        "departure_airport", "scheduled_departure", "actual_departure"
    )

    spark_df_flights = spark_df_flights.withColumn(
        "delay_seconds",
        unix_timestamp(col("actual_departure"))
        - unix_timestamp(col("scheduled_departure")),
    )
    spark_df_flights = spark_df_flights.withColumn(
        "scheduled_departure", date_format("scheduled_departure", "yyyy-MM-dd")
    )

    spark_df_flights = spark_df_flights.groupBy(
        "departure_airport", "scheduled_departure"
    ).agg(_avg("delay_seconds").alias("avg_delay_seconds"))

    spark_df_flights = spark_df_flights.withColumn(
        "avg_delay_seconds", func.round(spark_df_flights["avg_delay_seconds"], 2)
    )
    spark_df_flights = spark_df_flights.join(
        spark_df_airports_data,
        spark_df_flights["departure_airport"] == spark_df_airports_data["airport_code"],
        "left",
    )
    spark_df_flights = spark_df_flights.select(
        "scheduled_departure", "airport_name", "avg_delay_seconds"
    )
    spark_df_flights = spark_df_flights.withColumnRenamed("scheduled_departure", "date")

    return spark_df_flights


def occupancy(
    spark_df_flights, spark_df_seats, spark_df_ticket_flights, spark_df_airports_data
):
    """
    Creating aggregate of daily flight occupancy per airport.

    Args:
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_seats: Extracted table of seats from Bigquery converted to Pyspark Dataframe.
        spark_df_ticket_flights: Extracted table of flight tickets from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily flight occupancy per airport.
    """
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)

    spark_df_flights = spark_df_flights.na.drop(subset=["flight_id"])

    seats_per_aircraft = spark_df_seats.groupBy("aircraft_code").count()
    seats_per_aircraft = seats_per_aircraft.withColumnRenamed("count", "aircraft_seats")

    seats_per_flight = spark_df_ticket_flights.groupBy("flight_id").count()
    seats_per_flight = seats_per_flight.withColumnRenamed("count", "flight_passengers")

    spark_df_flights = spark_df_flights.join(
        seats_per_aircraft,
        spark_df_flights["aircraft_code"] == seats_per_aircraft["aircraft_code"],
        "left",
    )
    spark_df_flights = spark_df_flights.join(
        seats_per_flight,
        spark_df_flights["flight_id"] == seats_per_flight["flight_id"],
        "left",
    )

    spark_df_flights = spark_df_flights.na.drop(
        subset=["actual_departure", "actual_arrival"]
    )
    spark_df_flights = spark_df_flights.join(
        spark_df_airports_data,
        spark_df_flights["departure_airport"] == spark_df_airports_data["airport_code"],
        "left",
    )
    spark_df_flights = spark_df_flights.withColumn(
        "scheduled_departure", date_format("scheduled_departure", "yyyy-MM-dd")
    )
    spark_df_flights = spark_df_flights.withColumnRenamed("scheduled_departure", "date")

    spark_df_flights = spark_df_flights.groupBy("date", "airport_name").agg(
        _sum("flight_passengers").alias("passengers"),
        _sum("aircraft_seats").alias("seats_available"),
    )

    spark_df_flights = spark_df_flights.withColumn(
        "avg_occupancy_percent",
        _round((col("passengers") / col("seats_available")) * 100, 2),
    )
    spark_df_flights = spark_df_flights.drop("passengers", "seats_available")
    spark_df_flights = spark_df_flights.na.fill(
        value=0, subset=["avg_occupancy_percent"]
    )

    return spark_df_flights


def occupancy_fare_class(
    spark_df_seats,
    spark_df_aircrafts_data,
    spark_df_ticket_flights,
    spark_df_flights,
    spark_df_airports_data,
):
    """
    Creating aggregate of daily flight occupancy per fare class per airport.

    Args:
        spark_df_seats: Extracted table of seats from Bigquery converted to Pyspark Dataframe.
        spark_df_aircrafts_data: Extracted table of aircrafts from Bigquery converted to Pyspark Dataframe.
        spark_df_ticket_flights: Extracted table of flight tickets from Bigquery converted to Pyspark Dataframe.
        spark_df_flights: Extracted table of flights from Bigquery converted to Pyspark Dataframe.
        spark_df_airports_data: Extracted table of airports from Bigquery converted to Pyspark Dataframe.

    Returns:
        Pyspark Dataframe containing daily flight occupancy per fare class per airport.
    """
    
    spark_df_airports_data = fix_airports_data_df(spark_df_airports_data)
    
    seats_per_aircraft_economy_class = (
        spark_df_seats.filter(col("fare_conditions") == "Economy")
        .groupBy("aircraft_code")
        .count()
        .withColumnRenamed("count", "aircraft_seats_economy")
    )
    seats_per_aircraft_business_class = (
        spark_df_seats.filter(col("fare_conditions") == "Business")
        .groupBy("aircraft_code")
        .count()
        .withColumnRenamed("count", "aircraft_seats_business")
    )
    seats_per_aircraft_comfort_class = (
        spark_df_seats.filter(col("fare_conditions") == "Comfort")
        .groupBy("aircraft_code")
        .count()
        .withColumnRenamed("count", "aircraft_seats_comfort")
    )

    seats_final_df = (
        spark_df_aircrafts_data.select("aircraft_code")
        .distinct()
        .withColumnRenamed("aircraft_code", "code")
    )
    seats_final_df = (
        seats_final_df.join(
            seats_per_aircraft_economy_class.alias("economy"),
            seats_final_df["code"] == col("economy.aircraft_code"),
            "left",
        )
        .join(
            seats_per_aircraft_business_class.alias("business"),
            seats_final_df["code"] == col("business.aircraft_code"),
            "left",
        )
        .join(
            seats_per_aircraft_comfort_class.alias("comfort"),
            seats_final_df["code"] == col("comfort.aircraft_code"),
            "left",
        )
        .select(
            "code",
            "aircraft_seats_economy",
            "aircraft_seats_business",
            "aircraft_seats_comfort",
        )
        .na.fill(
            value=0,
            subset=[
                "aircraft_seats_business",
                "aircraft_seats_comfort",
                "aircraft_seats_economy",
            ],
        )
    )

    seats_per_flight_economy_class = (
        spark_df_ticket_flights.filter(col("fare_conditions") == "Economy")
        .groupBy("flight_id")
        .count()
        .withColumnRenamed("count", "flight_passengers_economy_class")
    )
    seats_per_flight_business_class = (
        spark_df_ticket_flights.filter(col("fare_conditions") == "Business")
        .groupBy("flight_id")
        .count()
        .withColumnRenamed("count", "flight_passengers_business_class")
    )
    seats_per_flight_comfort_class = (
        spark_df_ticket_flights.filter(col("fare_conditions") == "Comfort")
        .groupBy("flight_id")
        .count()
        .withColumnRenamed("count", "flight_passengers_comfort_class")
    )

    ticket_flights_final_df = (
        spark_df_ticket_flights.select("flight_id")
        .distinct()
        .withColumnRenamed("flight_id", "id")
    )
    ticket_flights_final_df = (
        ticket_flights_final_df.join(
            seats_per_flight_economy_class.alias("economy"),
            ticket_flights_final_df["id"] == col("economy.flight_id"),
            "left",
        )
        .join(
            seats_per_flight_business_class.alias("business"),
            ticket_flights_final_df["id"] == col("business.flight_id"),
            "left",
        )
        .join(
            seats_per_flight_comfort_class.alias("comfort"),
            ticket_flights_final_df["id"] == col("comfort.flight_id"),
            "left",
        )
        .select(
            "id",
            "flight_passengers_economy_class",
            "flight_passengers_business_class",
            "flight_passengers_comfort_class",
        )
        .withColumnRenamed(
            "flight_passengers_economy_class", "passengers_economy_class"
        )
        .withColumnRenamed(
            "flight_passengers_business_class", "passengers_business_class"
        )
        .withColumnRenamed(
            "flight_passengers_comfort_class", "passengers_comfort_class"
        )
        .na.fill(
            value=0,
            subset=[
                "passengers_economy_class",
                "passengers_business_class",
                "passengers_comfort_class",
            ],
        )
    )

    df_final = (
        spark_df_flights.na.drop(
            subset=["flight_id", "actual_departure", "actual_arrival"]
        )
        .join(
            seats_final_df.alias("seats"),
            col("aircraft_code") == col("seats.code"),
            "left",
        )
        .join(
            ticket_flights_final_df.alias("tickets"),
            col("flight_id") == col("tickets.id"),
            "left",
        )
        .select(
            "flight_id",
            date_format("scheduled_departure", "yyyy-MM-dd").alias("date"),
            "departure_airport",
            "aircraft_seats_economy",
            "aircraft_seats_business",
            "aircraft_seats_comfort",
            "passengers_economy_class",
            "passengers_business_class",
            "passengers_comfort_class",
        )
        .na.fill(
            value=0,
            subset=[
                "date",
                "departure_airport",
                "aircraft_seats_economy",
                "aircraft_seats_business",
                "aircraft_seats_comfort",
                "passengers_economy_class",
                "passengers_business_class",
                "passengers_comfort_class",
            ],
        )
        .join(
            spark_df_airports_data.alias("airports"),
            col("departure_airport") == col("airports.airport_code"),
            "left",
        )
        .groupBy("date", "airport_name")
        .agg(
            _sum("aircraft_seats_economy").alias("seats_economy"),
            _sum("aircraft_seats_business").alias("seats_business"),
            _sum("aircraft_seats_comfort").alias("seats_comfort"),
            _sum("passengers_economy_class").alias("passengers_economy"),
            _sum("passengers_business_class").alias("passengers_business"),
            _sum("passengers_comfort_class").alias("passengers_comfort"),
        )
        .withColumn(
            "avg_occupancy_economy",
            _round((col("passengers_economy") / col("seats_economy")) * 100, 2),
        )
        .withColumn(
            "avg_occupancy_business",
            _round((col("passengers_business") / col("seats_business")) * 100, 2),
        )
        .withColumn(
            "avg_occupancy_comfort",
            _round((col("passengers_comfort") / col("seats_comfort")) * 100, 2),
        )
        .select(
            "date",
            "airport_name",
            "avg_occupancy_economy",
            "avg_occupancy_business",
            "avg_occupancy_comfort",
        )
        .na.fill(
            value=0,
            subset=[
                "avg_occupancy_economy",
                "avg_occupancy_business",
                "avg_occupancy_comfort",
            ],
        )
    )

    return df_final
