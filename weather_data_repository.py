import pandas as pd
import sqlite3
from sqlite3 import Error

from contextlib import closing

class Repository:
    """Creates a connection to the database used to store weather data. Will create the database if it does not exist."""
    
    def __init__(self, sql_db_path):
        self._sql_db_path = sql_db_path
        self._connection_status = self.create_connection()

    def create_connection(self):
        """Creates a connection to the database. This creates a connection to the database if it exists, if it does not exist it will create it."""

        connection = None
        success = None

        try:
            connection = sqlite3.connect(self._sql_db_path)
            print("Connection to SQLite DB successful")
            success = True
        except Error as e:
            print(f"The error '{e}' occurred")
            success = False

        connection.close()

        return success
    
    def execute_query(self, query, data=[], headers=False):
        """Executes a query against the database. Returns a boolean representing the success/failure of the queries execution."""
        with closing(sqlite3.connect(self._sql_db_path)) as connection:
            with closing(connection.cursor()) as cursor:
                try:
                    if len(data) == 1:
                        cursor.execute(query, data[0])
                    elif len(data) > 1:
                        cursor.executemany(query, data)
                    else:
                        cursor.execute(query)
                    
                    connection.commit()

                    print("Query executed successfully")

                    if headers:
                        headers = list(map(lambda attr : attr[0], cursor.description))
                        results = [{header:row[i] for i, header in enumerate(headers)} for row in cursor]
                        return True, results
                    else:
                        return True, cursor.fetchall()

                except Error as e:
                    print(f"The error '{e}' occurred")

                    return False, None

    def create_weather_table(self):
        """Creates the weather table within the database if it does not already exist."""
        query = """
            CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL UNIQUE,
            outdoortemperature REAL,
            outdoorhumidity REAL,
            dewpoint REAL,
            heatindex REAL,
            windchill REAL,
            barometricpressure REAL,
            rain REAL,
            windspeed REAL,
            windaverage REAL,
            peakwind REAL,
            winddirection REAL,
            indoortemperature REAL,
            indoorhumidity REAL,
            timestampdateonly REAL
            );
        """
        status = self.execute_query(query)

        return status

    def insert_data(self, data):
        """Inserts WeatherData into the weather table in the database."""

        query = """
        INSERT INTO weather (
            timestamp, 
            outdoortemperature, 
            outdoorhumidity, 
            dewpoint, 
            heatindex, 
            windchill, 
            barometricpressure, 
            rain, 
            windspeed, 
            windaverage, 
            peakwind, 
            winddirection, 
            indoortemperature, 
            indoorhumidity,
            timestampdateonly
            )
        VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        """

        self.execute_query(query, data)

    def return_all_weather_data(self):
        """Returns all weather data found within the weather table in the database."""

        query = "SELECT * FROM weather"

        with closing(sqlite3.connect(self._sql_db_path)) as connection:
            with closing(connection.cursor()) as cursor:
                try:
                    cursor.execute(query)

                    results = cursor.fetchall()

                    print("Query executed successfully")

                    return (True, results)
                except Error as e:
                    print(f"The error '{e}' occurred")

                    return (False, None)
                
    def current_weather(self):
        """Returns the last record in the database representing the most current weather."""

        query = "SELECT * FROM weather ORDER BY id DESC LIMIT 1"

        return self.execute_query(query, headers=True)