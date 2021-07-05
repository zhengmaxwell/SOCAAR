from Postgres import Postgres



class Views:

    psql = None

    NAPS_CONTINUOUS = "naps_continuous_pollutant_concentrations_master"
    NAPS_INTEGRATED_CARBONYLS = "naps_integrated_carbonyls_pollutant_concentrations_master"
    NAPS_INTEGRATED_VOC = "naps_integrated_voc_pollutant_concentrations_master"
    NAPS_INTEGRATED_PAH = "naps_integrated_pah_pollutant_concentrations_master"
    NAPS_INTEGRATED_PM25 = "naps_integrated_pm25_pollutant_concentrations_master"
    NAPS_INTEGRATED_PM25_10 = "naps_integrated_pm25_10_pollutant_concentrations_master"

    
    @classmethod
    def connect(cls, psql: Postgres) -> None:

        cls.psql = psql


    # TODO: use variables and loops instead of hardcoding all this
    @staticmethod
    def create_naps_continuous() -> None:

        command = f"""
            CREATE OR REPLACE VIEW {Views.NAPS_CONTINUOUS} AS
                SELECT DISTINCT ON (total.timestamp, total.naps_station)
                    total.timestamp,
                    naps_stations.name as naps_station,
                    o3.density AS o3,
                    pm25.density AS pm25,
                    co.density AS co,
                    so2.density AS so2,
                    no2.density AS no2,
                    no.density AS no,
                    nox.density AS nox,
                    pm10.density AS pm10
                FROM naps_continuous_pollutant_concentrations total
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 1) o3
                    ON o3.timestamp = total.timestamp
                    AND o3.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 2) pm25
                    ON pm25.timestamp = total.timestamp
                    AND pm25.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations
                    WHERE pollutant = 3) no2
                    ON no2.timestamp = total.timestamp
                    AND no2.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 4) so2
                    ON so2.timestamp = total.timestamp
                    AND so2.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 5) co
                    ON co.timestamp = total.timestamp
                    AND co.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 6) no
                    ON no.timestamp = total.timestamp
                    AND no.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 7) nox
                    ON nox.timestamp = total.timestamp
                    AND nox.naps_station = total.naps_station
                FULL OUTER JOIN (
                    SELECT * FROM naps_continuous_pollutant_concentrations 
                    WHERE pollutant = 8) pm10
                    ON pm10.timestamp = total.timestamp
                    AND pm10.naps_station = total.naps_station
                INNER JOIN naps_stations
                    ON total.naps_station = naps_stations.id
        """
        Views.psql.command(command, 'w')

    # TODO: add all compounds as column names like the excel files
    @staticmethod
    def create_naps_integrated_pollutant(table: str, compounds_table, pollutant: str) -> None:

        view = Views._get_naps_integrated_pollutant_view(pollutant)

        command = f"""
            CREATE OR REPLACE VIEW {view} AS
                SELECT 
                    master.sampling_date,
                    stations.name AS naps_station,
                    sample_types.name AS sample_type,
                    compounds.name AS compound,
                    master.density,
                    master.density_mdl,
                    validation_codes.name AS vflag
                FROM {table} master
                INNER JOIN naps_stations stations
                    ON stations.id = master.naps_station
                INNER JOIN naps_sample_types sample_types
                    ON sample_types.id = master.sample_type
                INNER JOIN {compounds_table} compounds
                    ON compounds.id = master.compound
                INNER JOIN naps_validation_codes validation_codes
                    ON validation_codes.id = master.vflag
        """
        Views.psql.command(command, 'w')


    @staticmethod
    def _get_naps_integrated_pollutant_view(pollutant: str) -> str:

        if pollutant.upper() == "CARBONYLS":
            view = Views.NAPS_INTEGRATED_CARBONYLS
        elif pollutant.upper() == "VOC":
            view = Views.NAPS_INTEGRATED_VOC
        elif pollutant.upper() == "PAH":
            view = Views.NAPS_INTEGRATED_PAH
        elif pollutant.upper() == "PM2.5":
            view = Views.NAPS_INTEGRATED_PM25
        elif pollutant.upper() == "PM2.5-10":
            view = Views.NAPS_INEGRATED_PM25_10
        else:
            raise ValueError(f"pollutant {pollutant} not recognized")

        return view