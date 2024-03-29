from Postgres import Postgres
from Views import Views
import os
import json
import unicodedata
from typing import List



class Tables:

    psql = None
    
    MOE = "moe_pollutant_concentrations"
    NAPS_CONTINUOUS = "naps_continuous_pollutant_concentrations"
    NAPS_INTEGRATED_CARBONYLS = "naps_integrated_carbonyls_pollutant_concentrations"
    NAPS_INTEGRATED_VOC = "naps_integrated_voc_pollutant_concentrations"
    NAPS_INTEGRATED_PAH = "naps_integrated_pah_pollutant_concentrations"
    NAPS_INTEGRATED_PM25 = "naps_integrated_pm25_pollutant_concentrations"
    NAPS_INTEGRATED_PM25_10 = "naps_integrated_pm25_10_pollutants_concentrations"
    MOE_STATIONS = "moe_stations"
    NAPS_STATIONS = "naps_stations"
    NAPS_POLLUTANTS = "naps_pollutants"
    NAPS_VALIDATION_CODES = "naps_validation_codes"
    NAPS_SAMPLE_TYPES = "naps_sample_types"
    NAPS_ANALYTICAL_INSTRUMENTS = "naps_analytical_instruments"
    NAPS_OBSERVATION_TYPES = "naps_observation_types"
    NAPS_MEDIUMS = "naps_mediums"
    NAPS_SPECIATION_SAMPLER_CARTRIDGES = "naps_speciation_sampler_cartridges"
    NAPS_INTEGRATED_CARBONYLS_COMPOUNDS = "naps_integrated_carbonyls_compounds"
    NAPS_INTEGRATED_VOC_COMPOUNDS = "naps_integrated_voc_compounds"
    NAPS_INTEGRATED_PAH_COMPOUNDS = "naps_integrated_pah_compounds"
    NAPS_INTEGRATED_PM25_COMPOUNDS = "naps_integrated_pm25_compounds"
    NAPS_INTEGRATED_PM25_10_COMPOUNDS = "naps_integrated_pm25_10_compounds"

    # name: id
    seen_moe_stations = {}
    seen_naps_stations = {}
    seen_naps_pollutants = {}
    seen_naps_validation_codes = {}
    seen_naps_sample_types = {}
    seen_naps_analytical_instruments = {}
    seen_naps_observation_types = {}
    seen_naps_mediums = {}
    seen_naps_speciation_sampler_cartridges = {}
    seen_naps_integrated_carbonyls_compounds = {}
    seen_naps_integrated_voc_compounds = {}
    seen_naps_integrated_pah_compounds = {}
    seen_naps_integrated_pm25_compounds = {}
    seen_naps_integrated_pm25_10_compounds = {}

    integrated_pollutants = {
        "CARBONYLS": {
            "table": NAPS_INTEGRATED_CARBONYLS,
            "compounds_table": NAPS_INTEGRATED_CARBONYLS_COMPOUNDS,
            "seen_compounds": seen_naps_integrated_carbonyls_compounds 
        },
        "VOC": {
            "table": NAPS_INTEGRATED_VOC,
            "compounds_table": NAPS_INTEGRATED_VOC_COMPOUNDS,
            "seen_compounds": seen_naps_integrated_voc_compounds 
        },
        "PAH": {
            "table": NAPS_INTEGRATED_PAH,
            "compounds_table": NAPS_INTEGRATED_PAH_COMPOUNDS,
            "seen_compounds": seen_naps_integrated_pah_compounds 
        },
        "PM2.5": {
            "table": NAPS_INTEGRATED_PM25,
            "compounds_table": NAPS_INTEGRATED_PM25_COMPOUNDS,
            "seen_compounds": seen_naps_integrated_pm25_compounds 
        },
        "PM2.5-10": {
            "table": NAPS_INTEGRATED_PM25_10,
            "compounds_table": NAPS_INTEGRATED_PM25_10_COMPOUNDS,
            "seen_compounds": seen_naps_integrated_pm25_10_compounds
        }
    }

    @classmethod
    def connect(cls, psql: Postgres) -> None:

        cls.psql = psql
        Views.connect(psql)


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
                    FOREIGN KEY(moe_station) REFERENCES {Tables.MOE_STATIONS}(id)
                )
            """
            Tables.psql.command(command, 'w')


    @staticmethod
    def create_naps_continuous() -> None:

        Tables._create_naps_stations()
        Tables._create_naps_pollutants()

        if not Tables.psql.does_table_exist(Tables.NAPS_CONTINUOUS):
            command = f"""
                CREATE TABLE {Tables.NAPS_CONTINUOUS} (
                    id SERIAL PRIMARY KEY,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    timestamp TIMESTAMP NOT NULL,
                    naps_station INTEGER NOT NULL,
                    pollutant INTEGER NOT NULL,
                    density FLOAT,
                    FOREIGN KEY(naps_station) REFERENCES {Tables.NAPS_STATIONS}(id),
                    FOREIGN KEY(pollutant) REFERENCES {Tables.NAPS_POLLUTANTS}(id)
                )
            """
            Tables.psql.command(command, 'w')
            Views.create_naps_continuous()


    @staticmethod
    def create_naps_integrated_pollutant(pollutant: str) -> None:

        table = Tables.integrated_pollutants[pollutant]["table"]
        compounds = Tables.integrated_pollutants[pollutant]["compounds_table"]

        Tables._create_naps_stations()
        Tables._create_naps_metadata_tables()
        Tables._create_naps_integrated_pollutant_compounds(pollutant)

        if not Tables.psql.does_table_exist(table):
            command = f"""
                CREATE TABLE {table} (
                    id SERIAL PRIMARY KEY,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    sampling_date DATE NOT NULL,
                    naps_station INTEGER NOT NULL,
                    sample_type INTEGER,
                    compound INTEGER,
                    density FLOAT,
                    density_mdl FLOAT,
                    vflag INTEGER,
                    FOREIGN KEY(naps_station) REFERENCES {Tables.NAPS_STATIONS}(id),
                    FOREIGN KEY(sample_type) REFERENCES {Tables.NAPS_SAMPLE_TYPES}(id),
                    FOREIGN KEY(compound) REFERENCES {compounds}(id),
                    FOREIGN KEY(vflag) REFERENCES {Tables.NAPS_VALIDATION_CODES}(id)
                )
            """
            Tables.psql.command(command, 'w')
            Views.create_naps_integrated_pollutant(table, compounds, pollutant)


    # given a value will return the primary key id from the respective metadata table

    @classmethod
    def get_moe_station(cls, station: str) -> int:

        if station not in cls.seen_moe_stations:
            command = f"SELECT id FROM {cls.MOE_STATIONS} WHERE name = %(city)s"
            str_params = {"city": station}
            cls.seen_moe_stations[station] = cls.psql.command(command, 'r', str_params)[0][0]

        return cls.seen_moe_stations[station]


    @classmethod
    def get_naps_station(cls, station: int) -> int:

        if station not in cls.seen_naps_stations:
            command = f"SELECT id FROM {cls.NAPS_STATIONS} WHERE naps_id = {station}"
            cls.seen_naps_stations[station] = cls.psql.command(command, 'r')[0][0]

        return cls.seen_naps_stations[station]


    @classmethod
    def get_naps_pollutants(cls, pollutant: str) -> int:

        if pollutant not in cls.seen_naps_pollutants:
            command = f"SELECT id FROM {cls.NAPS_POLLUTANTS} WHERE name = %(pollutant)s"
            str_params = {"pollutant": pollutant}
            cls.seen_naps_pollutants[pollutant] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

        return cls.seen_naps_pollutants[pollutant]


    @classmethod
    def get_naps_validation_code(cls, validation_code: str) -> int:

            if validation_code not in cls.seen_naps_validation_codes:
                name = "is NULL" if validation_code == "NULL" else "= %(validation_code)s"
                command = f"SELECT id FROM {cls.NAPS_VALIDATION_CODES} WHERE name {name}"
                str_params = {"validation_code": validation_code}
                cls.seen_naps_validation_codes[validation_code] = cls.psql.command(command, 'r', str_params=str_params)[0][0]
                
            return cls.seen_naps_validation_codes[validation_code]

    
    @classmethod
    def get_naps_sample_type(cls, sample_type: str) -> int:

        if sample_type not in cls.seen_naps_sample_types:
            command = f"SELECT id FROM {cls.NAPS_SAMPLE_TYPES} WHERE name = %(sample_type)s"
            str_params = {"sample_type": sample_type}
            cls.seen_naps_sample_types[sample_type] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

        return cls.seen_naps_sample_types[sample_type]

    
    @classmethod
    def get_naps_analytical_instrument(cls, analytical_instrument: str) -> int:

        if analytical_instrument not in cls.seen_naps_analytical_instruments:
            command = f"SELECT id FROM {cls.NAPS_ANALYTICAL_INSTRUMENTS} WHERE name = %(analytical_instrument)s"
            str_params = {"analytical_instrument": analytical_instrument}
            cls.seen_naps_analytical_instruments[analytical_instrument] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

        return cls.seen_naps_analytical_instruments[analytical_instrument]


    @classmethod
    def get_naps_observation_type(cls, observation_type) -> int:

        if observation_type not in cls.seen_naps_observation_types:
            command = f"SELECT id FROM {cls.NAPS_OBSERVATION_TYPES} WHERE name = %(observation_type)s"
            str_params = {"observation_type": observation_type}
            cls.seen_naps_observation_types[observation_type] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

        return cls.seen_naps_observation_types[observation_type]


    @classmethod
    def get_naps_medium(cls, medium: str) -> int:

        if medium not in cls.seen_naps_mediums:
            command = f"SELECT id FROM {cls.NAPS_MEDIUMS} WHERE name = %(medium)s"
            str_params = {"medium": medium}
            cls.seen_naps_mediums[medium] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

        return cls.seen_naps_mediums[medium]


    @classmethod
    def get_naps_speciation_sampler_cartridge(cls, speciation_sampler_cartridge: str) -> int:

        if speciation_sampler_cartridge not in cls.seen_naps_speciation_sampler_cartridges:
            command = f"SELECT id FROM {cls.NAPS_SPECIATION_SAMPLER_CARTRIDGES} WHERE name = %(speciation_sampler_cartridge)s"
            str_params = {"speciation_sampler_cartridge": speciation_sampler_cartridge}
            cls.seen_naps_speciation_sampler_cartridges[speciation_sampler_cartridge] = cls.psql.command(command, 'r', str_params=str_params)[0][0]
        
        return cls.seen_naps_speciation_sampler_cartridges[speciation_sampler_cartridge]

    
    @staticmethod
    def get_naps_integrated_all_pollutant_compounds(pollutant: str) -> List[str]:

        table = Tables.integrated_pollutants[pollutant]["table"]

        command = f"SELECT name FROM {table}"
        return Tables.psql.command(command, 'r')

    
    @classmethod
    def get_naps_integrated_pollutant_compound(cls, pollutant: str, compound: str) -> int:

        table = Tables.integrated_pollutants[pollutant]["compounds_table"]
        seen = Tables.integrated_pollutants[pollutant]["seen_compounds"]

        # expecting: "compound-metadata:{key:val}"
        compound_name, metadata = compound.split("-metadata:")
        metadata = eval(metadata) # convert to dict
        units = None if "Units" not in metadata else metadata["Units"]
        medium = None if "Medium" not in metadata else metadata["Medium"]
        observation_type = None if "Observation Type" not in metadata else metadata["Observation Type"]
        analytical_instrument = None if "Analytical Instrument" not in metadata else metadata["Analytical Instrument"]
        speciation_sampler_cartridge = None if "Speciation Sampler Cartridge" not in metadata else metadata["Speciation Sampler Cartridge"]

        if compound_name not in seen:
            command = f"""
                SELECT compounds.id
                FROM {table} compounds
                FULL OUTER JOIN naps_mediums mediums
                    ON mediums.id = compounds.medium
                FULL OUTER JOIN naps_observation_types obs
                    ON obs.id = compounds.observation_type
                FULL OUTER JOIN naps_analytical_instruments ais
                    ON ais.id = compounds.analytical_instrument
                FULL OUTER JOIN naps_speciation_sampler_cartridges cartridges
                    ON cartridges.id = compounds.speciation_sampler_cartridge
                WHERE compounds.name = %(compound)s
            """
            if units:
                command += (" AND compounds.units = %(units)s")
            if medium:
                command += (" AND mediums.name = %(medium)s")
            if observation_type:
                command += (" AND obs.name = %(observation_type)s")
            if analytical_instrument:
                command += (" AND ais.name = %(analytical_instrument)s")
            if speciation_sampler_cartridge:
                command += (" AND cartridges.name = %(speciation_sampler_cartridge)s")

            str_params = {
                "compound": compound_name, 
                "units": units,
                "medium": medium, 
                "observation_type": observation_type, 
                "analytical_instrument": analytical_instrument,
                "speciation_sampler_cartridge": speciation_sampler_cartridge}
            seen[compound] = cls.psql.command(command, 'r', str_params=str_params)[0][0]

        return seen[compound]


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

            with open(f"{os.path.dirname(__file__)}/station_data/naps.json", 'r', encoding="latin-1") as naps_file:
                data = json.loads(naps_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_STATIONS} (naps_id, name, latitude, longitude)
                    VALUES ({row["NAPS_ID"]}, %(name)s, {row["Latitude"] or "Null"}, {row["Longitude"] or "Null"})
                """
                str_params = {"name": unicodedata.normalize("NFKD", row["Station_Name"]).encode("ASCII", "ignore").decode("utf-8")} # escape french chars
                Tables.psql.command(command, 'w', str_params=str_params)

    @staticmethod
    def _create_naps_pollutants() -> None:

        if not Tables.psql.does_table_exist(Tables.NAPS_POLLUTANTS):
            command = f"""
                CREATE TABLE {Tables.NAPS_POLLUTANTS} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/pollutants.json", 'r') as pollutants_file:
                data = json.loads(pollutants_file.read())

            for row in data:
                command = f"""
                    INSERT INTO {Tables.NAPS_POLLUTANTS} (name)
                    VALUES (%(name)s)
                """
                str_params = {"name": row["Pollutant"]}
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
                    name VARCHAR,
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
                    name VARCHAR,
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
                    name VARCHAR(3),
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
    def _create_naps_integrated_pollutant_compounds(pollutant: str) -> None:

        table = Tables.integrated_pollutants[pollutant]["compounds_table"]

        if not Tables.psql.does_table_exist(table):
            command = f"""
                CREATE TABLE {table} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    units VARCHAR,
                    medium INTEGER,
                    observation_type INTEGER,
                    analytical_instrument INTEGER,
                    speciation_sampler_cartridge INTEGER,
                    FOREIGN KEY(medium) REFERENCES {Tables.NAPS_MEDIUMS}(id),
                    FOREIGN KEY(observation_type) REFERENCES {Tables.NAPS_OBSERVATION_TYPES}(id),
                    FOREIGN KEY(analytical_instrument) REFERENCES {Tables.NAPS_ANALYTICAL_INSTRUMENTS}(id),
                    FOREIGN KEY(speciation_sampler_cartridge) REFERENCES {Tables.NAPS_SPECIATION_SAMPLER_CARTRIDGES}(id)
                )
            """
            Tables.psql.command(command, 'w')

            with open(f"{os.path.dirname(__file__)}/naps_data/{pollutant.lower().replace('.', '')}_compounds.json", 'r') as compounds_file:
                data = json.loads(compounds_file.read())

            for row in data:
                # TODO: maybe these values can be 'NULL' as well
                medium = Tables.get_naps_medium(row["Medium"])
                observation_type = Tables.get_naps_observation_type(row["Observation Type"])
                analytical_instrument = Tables.get_naps_analytical_instrument(row["Analytical Instrument"])

                if row["Speciation Sampler Cartridge"] == "NULL":
                    speciation_sampler_cartridge = "NULL"
                else:
                    speciation_sampler_cartridge =  Tables.get_naps_speciation_sampler_cartridge(row["Speciation Sampler Cartridge"])

                command = f"""
                    INSERT INTO {table} (name, units, medium, observation_type, analytical_instrument, speciation_sampler_cartridge)
                    VALUES (%(name)s, %(units)s, {medium}, {observation_type}, {analytical_instrument}, {speciation_sampler_cartridge})
                """
                str_params = {"name": row["Compound"], "units": row["Units"]}
                Tables.psql.command(command, 'w', str_params=str_params)


    @staticmethod
    def _create_naps_metadata_tables() -> None:

        Tables._create_naps_validation_codes()
        Tables._create_naps_sample_types()
        Tables._create_naps_analytical_instruments()
        Tables._create_naps_observation_types()
        Tables._create_naps_mediums()
        Tables._create_naps_speciation_sampler_cartridges()