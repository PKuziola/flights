import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs import flight_job

class TestAggregates:
    """
    This class is used to test generating of the aggregates.
    """

    @pytest.fixture
    def create_spark_session(self):
        """
        Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            SparkSession object with the application name "agg_test".
        """
        return SparkSession.builder.appName('agg_test').getOrCreate()

    @pytest.fixture
    def aircrafts_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for aircrafts table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                ('773', '{"en": "Boeing 777-300", "ru": "Боинг 777-300"}', 11100),
                ('321', '{"en": "Airbus A321-200", "ru": "Аэробус A321-200"}', 5600),
                ('320', '{"en": "Airbus A320-200", "ru": "Аэробус A320-200"}', 5700)
            ],
            ['aircraft_code', 'model', 'range']
    )

    @pytest.fixture
    def airports_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for airports table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                ('DME', '{"en": "Domodedovo International Airport", "ru": "Домодедово"}',
                '{"en": "Moscow", "ru": "Москва"}',
                (37.90629959106445, 55.40879821777344), 'Europe/Moscow'),
                ('LED', '{"en": "Pulkovo Airport", "ru": "Пулково"}',
                '{"en": "St. Petersburg", "ru": "Санкт-Петербург"}',
                (30.262500762939453, 59.80030059814453), 'Europe/Moscow'),
                ('VVO', '{"en": "Vladivostok International Airport", "ru": "Владивосток"}',
                '{"en": "Vladivostok", "ru": "Владивосток"}',
                (132.1479949951172,43.39899826049805), 'Asia/Vladivostok'),
                ('SVO', '{"en": "Sheremetyevo International Airport", "ru": "Шереметьево"}',
                '{"en": "Moscow", "ru": "Москва"}',
                (37.4146,55.972599), 'Europe/Moscow'),
            ],
            ["airport_code", "airport_name", "city", "coordinates", "timezone"]
        )

    @pytest.fixture
    def bookings_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for bookings table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                ('000004', datetime.strptime('2020-01-30 08:25:00', '%Y-%m-%d %H:%M:%S'), 55800.00),
                ('00000F', datetime.strptime('2020-01-30 15:25:00', '%Y-%m-%d %H:%M:%S'), 265700.00),
                ('0000010', datetime.strptime('2020-01-30 22:25:00', '%Y-%m-%d %H:%M:%S'), 50900.00),
                ('000012', datetime.strptime('2020-01-31 02:25:00', '%Y-%m-%d %H:%M:%S'), 37900.00),
                ('000026', datetime.strptime('2020-01-31 15:25:00', '%Y-%m-%d %H:%M:%S'), 95600.00),
                ('00002D', datetime.strptime('2020-01-31 15:25:00', '%Y-%m-%d %H:%M:%S'), 114700.00),
            ],
            ["book_ref", "book_date", "total_amount"]
        )

    @pytest.fixture
    def flights_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for flights table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                (15698, 'AB0102', datetime.strptime('2020-01-30 08:25:00', '%Y-%m-%d %H:%M:%S'),
                datetime.strptime('2020-01-30 14:33:00', '%Y-%m-%d %H:%M:%S'), 'SVO', 'VVO', 'Arrived', '773',
                datetime.strptime('2020-01-30 08:27:00', '%Y-%m-%d %H:%M:%S'),
                datetime.strptime('2020-01-30 14:42:00', '%Y-%m-%d %H:%M:%S')),
                (22333, 'AB0203', datetime.strptime('2020-01-30 16:25:00', '%Y-%m-%d %H:%M:%S'),
                datetime.strptime('2020-01-30 18:25:00', '%Y-%m-%d %H:%M:%S'), 'LED', 'DME', 'Arrived', '321',
                datetime.strptime('2020-01-30 16:25:00', '%Y-%m-%d %H:%M:%S'),
                datetime.strptime('2020-01-30 18:35:00', '%Y-%m-%d %H:%M:%S')),
                (22433, 'AB0203', datetime.strptime('2020-01-31 05:00:00', '%Y-%m-%d %H:%M:%S'),
                datetime.strptime('2020-01-31 06:00:00', '%Y-%m-%d %H:%M:%S'), 'SVO', 'LED', 'Arrived', '321',
                datetime.strptime('2020-01-31 06:00:00', '%Y-%m-%d %H:%M:%S'),
                datetime.strptime('2020-01-31 07:00:00', '%Y-%m-%d %H:%M:%S')),
            ],
            [
                "flight_id", "flight_no", "scheduled_departure", "scheduled_arrival", "departure_airport",
                "arrival_airport", "status", "aircraft_code", "actual_departure", "actual_arrival"
            ]
        )


    @pytest.fixture
    def seats_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for seats table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                ('321', '2A', 'Economy'),
                ('321', '3A', 'Economy'),
                ('321', '3B', 'Comfort'),
                ('321', '33A', 'Business'),
                ('321', '33B', 'Economy'),
                ('773', '3B', 'Economy'),
                ('773', '3C', 'Economy'),
                ('773', '8A', 'Business'),
            ],
            ["aircraft_code", "seat_no", "fare_conditions"]
        )

    @pytest.fixture
    def ticket_flights_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for ticket_flights table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                ('0005435981740', 15698, 'Economy', 5500.00),
                ('0005435981741', 15698, 'Business', 19200.00),
                ('0005435981743', 15698, 'Economy', 9500.00),
                ('0005435981744', 22333, 'Economy', 5500.00),
                ('0005435981746', 22333, 'Comfort', 10000.00),
                ('0005435981748', 22433, 'Economy', 10000.00),          
            ],
            ['ticket_no', 'flight_id', 'fare_conditions', 'amount']
        )

    @pytest.fixture
    def tickets_test_data(self,create_spark_session):
        """
        Pytest fixture that returns a PySpark DataFrame for tickets table.

        Args:
            create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.

        Returns:
            PySpark DataFrame.
        """
        return create_spark_session.createDataFrame(
            [
                ('0005435981740', '000004', '2761 658276', 'IRINA KOZLOVA',
                '{"email": "kozlova_i-031980@postgrespro.ru", "phone": "+70555875224"}'),
                ('0005435981741', '00000F', '3786 388525', 'ANDREY ZAYCEV',
                '{"phone": "+70451935914"}'),
                ('0005435981743', '0000010', '7879 114492', 'NINA LUKYANOVA',
                '{"phone": "+70326667306"}'),
                ('0005435981744', '000012', '7879 114498', 'PIOTR PULEV',
                '{"phone": "+70326667333"}'),
                ('0005435981746', '000026', '7879 114493', 'KATRINA KOKONOVSKA',
                '{"phone": "+70326667355"}'),
                ('0005435981748', '00002D', '7855 114492', 'ALBERTO SIMAO',
                '{"phone": "+70326667301"}'),
            ],
            ['ticket_no', 'book_ref', 'passenger_id', 'passenger_name', 'contact_data']
        )

    def test_agg(
            self,
            create_spark_session,
            aircrafts_test_data,
            airports_test_data,
            bookings_test_data,
            flights_test_data,
            seats_test_data,
            ticket_flights_test_data,
            tickets_test_data             
                ):
            """
            PyTest for 'get_aggregates' function from flight_job.py file.
            Test ensures that all aggreagates have correct values.
            
            Args:
                create_spark_session: Pytest fixture that creates a SparkSession object for testing purposes.
                aircrafts_test_data: PySpark DataFrame for aircrafts table.
                airports_test_data: PySpark DataFrame for airports table.
                bookings_test_data: PySpark DataFrame for bookings table.
                flights_test_data: PySpark DataFrame for flights table.
                seats_test_data: PySpark DataFrame for seats table.
                ticket_flights_test_data: PySpark DataFrame for ticket_flights table.
                tickets_test_data: PySpark DataFrame for tickets table.

            Returns:
                None
            """
            expected_df = pd.DataFrame(
                {
                    "date": [
                        "2020-01-30","2020-01-30",
                        "2020-01-30","2020-01-30",
                        "2020-01-31","2020-01-31",
                        "2020-01-31","2020-01-31"
                        ],
                    "airport_name": [
                        "Domodedovo International Airport", "Pulkovo Airport", 
                        "Sheremetyevo International Airport", "Vladivostok International Airport",
                        "Domodedovo International Airport", "Pulkovo Airport", 
                        "Sheremetyevo International Airport", "Vladivostok International Airport"
                        ],
                    "bookings": [0,0,3,0,0,2,1,0],
                    "revenue": [0,15500,34200,0,0,0,10000,0],
                    "airtime_minutes": [0.0,130.0,375.0,0.0,0.0,0.0,60.0,0.0],
                    "flights_early_morning": [0,0,0,0,0,0,1,0],
                    "flights_morning": [0,0,1,0,0,0,0,0],
                    "flights_afternoon": [0,1,0,0,0,0,0,0],
                    "flights_evening": [0,0,0,0,0,0,0,0],
                    "avg_delay_seconds": [0.0,0.0,120.0,0.0,0.0,0.0,3600.00,0.0],
                    "avg_occupancy_percent": [0.00,40.00,100.00,0.00,0.00,0.00,20.00,0.00],
                    "avg_occupancy_economy": [0,33.33,100.00,0,0,0.00,33.33,0],
                    "avg_occupancy_business": [0,0,100.00,0,0,0,0,0],
                    "avg_occupancy_comfort": [0,100.00,0,0,0,0.00,0,0]
                }
            )

            real_df = flight_job.get_aggregates(
                aircrafts_test_data,
                airports_test_data,
                bookings_test_data,
                flights_test_data,
                seats_test_data,
                ticket_flights_test_data,
                tickets_test_data
            ).toPandas()

            pd.testing.assert_frame_equal(expected_df, real_df, check_dtype=True)

            create_spark_session.stop()