from Postgres import Postgres
import os
import json

class Tables:

    MOE = "moe_pollutant_concentrations"
    MOE_STATIONS = "moe_stations"
    NAPS_STATIONS = "naps_stations"
    NAPS_CONTINUOUS = "naps_continuous_pollutant_concentrations"
    NAPS_INTEGRATED_CARBONYLYS = "naps_integrated_carbonyls_pollutant_concentrations"

    @staticmethod
    def create_moe_stations(psql: Postgres) -> None:

        if not psql.does_table_exist(Tables.MOE_STATIONS):
                moe_file = open(f"{os.path.dirname(__file__)}/station_data/moe.json", 'r')
                data = json.loads(moe_file.read())

                command = f"""
                    CREATE TABLE {Tables.MOE_STATIONS} (
                        id SERIAL PRIMARY KEY,
                        moe_id INTEGER NOT NULL,
                        name VARCHAR NOT NULL,
                        latitude FLOAT NOT NULL,
                        longitude FLOAT NOT NULL
                    )
                """
                psql.command(command, 'w')

                for row in data:
                    command = f"""
                        INSERT INTO {Tables.MOE_STATIONS} (moe_id, name, latitude, longitude)
                        VALUES ({row["MOE ID"]}, %(name)s, {row["LATITUDE"]}, {row["LONGITUDE"]})
                        """
                    str_params = {"name": row["AQHI STATION NAME"]}
                    psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def create_naps_stations(psql: Postgres) -> None:

        if not psql.does_table_exist(Tables.NAPS_STATIONS):
            naps_file = open(f"{os.path.dirname(__file__)}/station_data/naps.json", 'r')
            data = json.loads(naps_file.read())

            command = f"""
                CREATE TABLE {Tables.NAPS_STATIONS} (
                    id SERIAL PRIMARY KEY,
                    naps_id INTEGER NOT NULL,
                    name VARCHAR NOT NULL,
                    latitude FLOAT,
                    longitude FLOAT
                )
            """
            psql.command(command, 'w')

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_STATIONS} (naps_id, name, latitude, longitude)
                    VALUES ({row["NAPS_ID"]}, %(name)s, {row["Latitude"] or "Null"}, {row["Longitude"] or "Null"})
                """
                str_params = {"name": row["Station_Name"]}
                psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def create_moe(psql: Postgres) -> None:

        if not psql.does_table_exist(Tables.MOE):
            command = f"""
                CREATE TABLE {Tables.MOE} (
                    id SERIAL PRIMARY KEY,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    year INTEGER NOT NULL CHECK (year <= DATE_PART('year', now())),
                    month INTEGER NOT NULL CHECK (month >= 1 and month <= 12),
                    day INTEGER NOT NULL CHECK (day >= 1 and day <= 31),
                    hour INTEGER NOT NULL CHECK (hour >= 0 and hour <= 23),
                    moe_station INTEGER NOT NULL,
                    o3 FLOAT,
                    pm2_5 FLOAT,
                    no2 FLOAT,
                    so2 FLOAT,
                    co FLOAT,
                    FOREIGN KEY(moe_station) REFERENCES moe_stations(id)
                )
            """
            psql.command(command, 'w')

    @staticmethod
    def create_naps_continuous(psql: Postgres) -> None:

        if not psql.does_table_exist(Tables.NAPS_CONTINUOUS):
            command = f"""
                CREATE TABLE {Tables.NAPS_CONTINUOUS} (
                    id SERIAL PRIMARY KEY,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    year INTEGER NOT NULL CHECK (year <= DATE_PART('year', now())),
                    month INTEGER NOT NULL CHECK (month >= 1 and month <= 12),
                    day INTEGER NOT NULL CHECK (day >= 1 and day <= 31),
                    hour INTEGER NOT NULL CHECK (hour >=0 and hour <= 23),
                    naps_station INTEGER NOT NULL,
                    co FLOAT,
                    no FLOAT,
                    no2 FLOAT,
                    nox FLOAT,
                    o3 FLOAT,
                    pm10 FLOAT,
                    pm25 FLOAT,
                    so2 FLOAT,
                    FOREIGN KEY(naps_station) REFERENCES naps_stations(id)
                )
            """
            psql.command(command, 'w')