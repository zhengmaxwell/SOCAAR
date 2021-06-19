from Postgres import Postgres



class Views:


    NAPS_CONTINUOUS = "naps_continuous_pollutant_concentrations_master"


    @staticmethod
    def create_naps_continuous(psql: Postgres) -> None:

        command = f"""
            CREATE OR REPLACE VIEW {Views.NAPS_CONTINUOUS} AS
               SELECT DISTINCT ON (total.year, total.month, total.day, total.hour, total.naps_station)
                        total.year,
                        total.month,
                        total.day,
                        total.hour,
                        total.naps_station,
                        o3.density as o3,
                        pm25.density as pm25,
                        co.density as no2,
                        so2.density as so2,
                        no2.density as co,
                        no.density as no,
                        nox.density as nox,
                        pm10.density as pm10
                    FROM naps_continuous_pollutant_concentrations total
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 1) o3
                        ON o3.year = total.year 
                        AND o3.month = total.month 
                        AND o3.day = total.day 
                        AND o3.hour = total.hour 
                        AND o3.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 2) pm25
                        ON pm25.year = total.year 
                        AND pm25.month = total.month 
                        AND pm25.day = total.day 
                        AND pm25.hour = total.hour 
                        AND pm25.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations
                        WHERE pollutant = 3) no2
                        ON no2.year = total.year 
                        AND no2.month = total.month 
                        AND no2.day = total.day 
                        AND no2.hour = total.hour 
                        AND no2.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 4) so2
                        ON so2.year = total.year 
                        AND so2.month = total.month 
                        AND so2.day = total.day 
                        AND so2.hour = total.hour 
                        AND so2.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 5) co
                        ON co.year = total.year 
                        AND co.month = total.month 
                        AND co.day = total.day 
                        AND co.hour = total.hour 
                        AND co.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 6) no
                        ON no.year = total.year 
                        AND no.month = total.month 
                        AND no.day = total.day 
                        AND no.hour = total.hour 
                        AND no.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 7) nox
                        ON nox.year = total.year 
                        AND nox.month = total.month 
                        AND nox.day = total.day 
                        AND nox.hour = total.hour 
                        AND nox.naps_station = total.naps_station
                    FULL OUTER JOIN (
                        SELECT * FROM naps_continuous_pollutant_concentrations 
                        WHERE pollutant = 8) pm10
                        ON pm10.year = total.year 
                        AND pm10.month = total.month 
                        AND pm10.day = total.day 
                        AND pm10.hour = total.hour 
                        AND pm10.naps_station = total.naps_station
        """
        psql.command(command, 'w')