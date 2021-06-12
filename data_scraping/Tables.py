from Postgres import Postgres
import os
import json
from typing import Union



class Tables:

    psql = None
    
    MOE = "moe_pollutant_concentrations"
    NAPS_CONTINUOUS = "naps_continuous_pollutant_concentrations"
    NAPS_INTEGRATED_CARBONYLYS = "naps_integrated_carbonyls_pollutant_concentrations"
    MOE_STATIONS = "moe_stations"
    NAPS_STATIONS = "naps_stations"
    NAPS_VALIDATION_CODES = "naps_validation_codes"
    NAPS_SAMPLE_TYPES = "naps_sample_types"
    NAPS_ANALYTICAL_INSTRUMENTS = "naps_analytical_instruments"
    NAPS_OBSERVATION_TYPES = "naps_observation_types"
    NAPS_MEDIUMS = "naps_mediums"
    NAPS_SPECIATION_SAMPLER_CARTRIDGES = "naps_speciation_sampler_cartridges"
    NAPS_INTEGRATED_CARBONYLS_COMPOUNDS = "naps_integrated_carbonyls_compounds"

    # name: id
    seen_naps_observation_types = {}
    seen_naps_mediums = {}
    seen_naps_analytical_instruments = {}

    @classmethod
    def connect(cls, psql: Postgres) -> None:

        cls.psql = psql


    @staticmethod
    def create_moe() -> None:

        Tables._create_moe_stations()

        if not Tables.psql.does_table_exist(Tables.MOE):
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
            Tables.psql.command(command, 'w')


    @staticmethod
    def create_naps_continuous() -> None:

        Tables._create_naps_stations()

        if not Tables.psql.does_table_exist(Tables.NAPS_CONTINUOUS):
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
            Tables.psql.command(command, 'w')

    @staticmethod
    def create_naps_integrated_carbonyls() -> None:

        Tables._create_naps_stations()
        Tables._create_naps_metadata_tables()

        if not Tables.psql.does_table_exist(Tables.NAPS_INTEGRATED_CARBONYLYS):
            command = f"""
                CREATE TABLE {Tables.NAPS_INTEGRATED_CARBONYLYS} (
                    id SERIAL PRIMARY KEY,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    sampling_date DATE NOT NULL,
                    naps_stations INTEGER NOT NULL,
                    sampling_type INTEGER,
                    formaldehyde FLOAT,
                    formaldehyde_mdl FLOAT,
                    formaldehyde_vflag INTEGER,
                    acetaldehyde FLOAT,
                    acetaldehyde_mdl FLOAT,
                    acetaldehyde_vflag INTEGER,
                    acetone FLOAT,
                    acetone_mdl FLOAT,
                    acdetone_vflag INTEGER,
                    propionaldehyde FLOAT,
                    propionaldehyde_mdl FLOAT,
                    propionaldehyde_vflag INTEGER,
                    crotonaldehyde FLOAT,
                    crotonaldehyde_mdl FLOAT,
                    crotonaldehyde_vflag INTEGER,
                    mek FLOAT,
                    mek_mdl FLOAT,
                    mek_vflag INTEGER,
                    butyraldehyde/iso-butyraldehyde FLOAT,
                    butyraldehyde/iso-butyraldehyde_mdl FLOAT,
                    butyraldehyde/iso-butyraldehyde_vflag INTEGER,
                    benzaldehyde FLOAT,
                    benzaldehyde_mdl FLOAT,
                    benzaldehyde_vflag INTEGER,
                    isovaleraldehyde FLOAT,
                    isovaleraldehyde_mdl FLOAT,
                    isovaleraldehyde_vflag INTEGER,
                    valeraldehyde FLOAT,
                    valeraldehyde_mdl FLOAT,
                    valeraldehyde_vflag INTEGER,
                    m-tolualdehyde FLOAT,
                    m-tolualdehyde_mdl FLOAT,
                    m-tolualdehyde_vflag INTEGER,
                    p-tolualdehyde FLOAT,
                    p-tolualdehyde_mdl FLOAT,
                    p-tolualdehyde_vflag INTEGER,
                    mibk FLOAT,
                    mibk_mdl FLOAT,
                    mibk_vflag,
                    hexanal FLOAT,
                    hexanal_mdl FLOAT,
                    hexanal_vflag INTEGER


                )

            """

    @staticmethod
    def _create_moe_stations() -> None:

        if not Tables.psql.does_table_exist(Tables.MOE_STATIONS):
                command = f"""
                    CREATE TABLE {Tables.MOE_STATIONS} (
                        id SERIAL PRIMARY KEY,
                        moe_id INTEGER NOT NULL,
                        name VARCHAR NOT NULL,
                        latitude FLOAT NOT NULL,
                        longitude FLOAT NOT NULL
                    )
                """
                Tables.psql.command(command, 'w')

                with open(f"{os.path.dirname(__file__)}/station_data/moe.json", 'r') as moe_file:
                    data = json.loads(moe_file.read())

                for row in data:
                    command = f"""
                        INSERT INTO {Tables.MOE_STATIONS} (moe_id, name, latitude, longitude)
                        VALUES ({row["MOE ID"]}, %(name)s, {row["LATITUDE"]}, {row["LONGITUDE"]})
                        """
                    str_params = {"name": row["AQHI STATION NAME"]}
                    Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_stations() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_STATIONS):
            command = f"""
                CREATE TABLE {Tables.NAPS_STATIONS} (
                    id SERIAL PRIMARY KEY,
                    naps_id INTEGER NOT NULL,
                    name VARCHAR NOT NULL,
                    latitude FLOAT,
                    longitude FLOAT
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/station_data/naps.json", 'r') as naps_file:
                data = json.loads(naps_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_STATIONS} (naps_id, name, latitude, longitude)
                    VALUES ({row["NAPS_ID"]}, %(name)s, {row["Latitude"] or "Null"}, {row["Longitude"] or "Null"})
                """
                str_params = {"name": row["Station_Name"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_validation_codes() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_VALIDATION_CODES):
            command = f"""
                CREATE TABLE {Tables.NAPS_VALIDATION_CODES} (
                    id SERIAL PRIMARY KEY,
                    name CHAR(2),
                    description VARCHAR NOT NULL
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/validation_codes.json", 'r') as validation_codes_file:
                data = json.loads(validation_codes_file.read())

            for row in data:
                name = "%(name)s" if row["Validation Code"] else "Null"
                command = f"""
                    INSERT INTO {Tables.NAPS_VALIDATION_CODES} (name, description)
                    VALUES ({name}, %(description)s)
                """
                str_params = {"name": row["Validation Code"], "description": row["Description"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_sample_types() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_SAMPLE_TYPES):
            command = f"""
                CREATE TABLE {Tables.NAPS_SAMPLE_TYPES} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(2),
                    description VARCHAR
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/sample_types.json", 'r') as sample_types_file:
                data = json.loads(sample_types_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_SAMPLE_TYPES} (name, description)
                    VALUES (%(name)s, %(description)s)
                """
                str_params = {"name": row["Sample Type"], "description": row["Description"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_analytical_instruments() -> None:

        # GC/MS exists in analytical_instruments.json
        # GC-MS is a duplicate must look for in .xlsx files (PAH)

        if not Tables.psql.does_table_exist(Tables.NAPS_ANALYTICAL_INSTRUMENTS):
            command = f"""
                CREATE TABLE {Tables.NAPS_ANALYTICAL_INSTRUMENTS} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    description VARCHAR
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/analytical_instruments.json", 'r') as analytical_instruments_file:
                data = json.loads(analytical_instruments_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_ANALYTICAL_INSTRUMENTS} (name, description)
                    VALUES (%(name)s, %(description)s)
                """
                str_params = {"name": row["Analytical Instrument"], "description": row["Description"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_observation_types() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_OBSERVATION_TYPES):
            command = f"""
                CREATE TABLE {Tables.NAPS_OBSERVATION_TYPES} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(2),
                    description VARCHAR
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/observation_types.json", 'r') as observation_types_file:
                data = json.loads(observation_types_file.read())
                
            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_OBSERVATION_TYPES} (name, description)
                    VALUES (%(name)s, %(description)s)
                """
                str_params = {"name": row["Observation Type"], "description": row["Description"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_mediums() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_MEDIUMS):
            command = f"""
                CREATE TABLE {Tables.NAPS_MEDIUMS} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(4),
                    description VARCHAR
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/mediums.json", 'r') as mediums_file:
                data = json.loads(mediums_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_MEDIUMS} (name, description)
                    VALUES (%(name)s, %(description)s)
                """
                str_params = {"name": row["Medium"], "description": row["Description"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_speciation_sampler_cartridges() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_SPECIATION_SAMPLER_CARTRIDGES):
            command = f"""
                CREATE TABLE {Tables.NAPS_SPECIATION_SAMPLER_CARTRIDGES} (
                    id SERIAL PRIMARY KEY,
                    name CHAR(1),
                    description VARCHAR
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/speciation_sampler_cartridges.json", 'r') as speciation_sampler_cartridges_file:
                data = json.loads(speciation_sampler_cartridges_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_SPECIATION_SAMPLER_CARTRIDGES} (name, description)
                    VALUES (%(name)s, %(description)s)
                """
                str_params = {"name": row["Speciation Sampler Cartridge"], "description": row["Description"]}
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_integrated_carbonyls_compounds() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_INTEGRATED_CARBONYLS_COMPOUNDS):
            command = f"""
                CREATE TABLE {Tables.NAPS_INTEGRATED_CARBONYLS_COMPOUNDS} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    medium INTEGER,
                    obsevation_type INTEGER,
                    analytical_instrument INTEGER,
                    FOREIGN KEY(medium) REFERENCES naps_mediums(id),
                    FOREIGN KEY(observation_type) REFERENCES naps_mediums(id),
                    FOREIGN KEY(analytical_instrument) REFERENCES naps_analytical_instruments(id)
                )
            """

            with open(f"{os.path.dirname(__file__)}/naps_data/carbonyls_compounds.json", 'r') as carbonyls_compounds_file:
                data = json.loads(carbonyls_compounds_file.read())

            for row in data:
                medium = Tables._get_naps_mediums(row["Medium"])
                observation_type = Tables._get_naps_observation_types(row["Observation Type"])
                analytical_instrument = Tables._get_naps_analytical_instruments(row["Analytical Instrument"])

                command = f"""
                    INSERT INTO {Tables.NAPS_INTEGRATED_CARBONYLS_COMPOUNDS} (name, medium, observation_type, analytical_instrument)
                    VALUES (%(name)s, {medium}, {observation_type}, {analytical_instrument})
                """
                str_params = {"name": row["Compound"]}
                Tables.psql.command(command, 'w')

    @classmethod
    def _get_naps_mediums(cls, medium: str) -> Union[int, None]:

        if medium in cls.seen_naps_mediums:
            return cls.seen_naps_mediums[medium]

        else:
            command = f"SELECT id FROM {cls.NAPS_MEDIUMS} WHERE name = %(medium)s"
            str_params = {"medium": medium}
            cls.seen_naps_mediums[medium] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

    @classmethod
    def _get_naps_observation_types(cls, observation_type) -> Union[int, None]:

        if observation_type in cls.seen_naps_observation_types:
            return cls.seen_naps_observation_types[observation_type]

        else:
            command = f"SELECT id FROM {cls.NAPS_OBSERVATION_TYPES} WHERE name = %(observation_type)s"
            str_params = {"observation_type": observation_type}
            cls.seen_naps_observation_types[observation_type] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

    @classmethod
    def _get_naps_analytical_instruments(cls, analytical_instrument: str) -> Union[int, None]:

        if analytical_instrument in cls.seen_naps_analytical_instruments:
            return cls.seen_naps_analytical_instruments[analytical_instrument]

        else:
            command = f"SELECT id FROM {cls.NAPS_ANALYTICAL_INSTRUMENTS} WHERE name = %(analytical_instrument)s"
            str_params = {"analytical_instrument": analytical_instrument}
            cls.seen_naps_analytical_instruments[analytical_instrument] = cls.psql.command(command, 'r', str_params=str_params)[0][0]


    @staticmethod
    def _create_naps_metadata_tables() -> None:

        Tables._create_naps_validation_codes()
        Tables._create_naps_sample_types()
        Tables._create_naps_analytical_instruments()
        Tables._create_naps_observation_types()
        Tables._create_naps_mediums()
        Tables._create_naps_speciation_sampler_cartridges()

    @staticmethod
    def test():
        Tables._create_moe_stations()
        Tables.create_moe()