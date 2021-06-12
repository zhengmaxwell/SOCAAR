from Postgres import Postgres
from Tables import Tables
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import timedelta, datetime
from typing import Dict, List



class MOE_Pollutant_Concentrations():


    def __init__(self, host: str, database: str, user: str, password: str) -> None:

        self._psql = Postgres(host, database, user, password)

        self._create_tables()


    def update_data(self) -> None:

        valid_time_range = self._get_valid_time_range()

        for t in valid_time_range:
            year, month, day, hour = t.year, t.month, t.day, t.hour
            self._insert_data(year, month, day, hour)

    def _create_tables(self) -> None:

        Tables.connect(self._psql)
        Tables.create_moe()

    def _insert_data(self, year: int, month: int, day: int, hour: int) -> None:

        data = self._get_data(year, month, day, hour)

        for city in data:
            
            moe_station_id = Tables.get_moe_station(city)

            command = f"""
                INSERT INTO {Tables.MOE} (year, month, day, hour, moe_station, o3, pm2_5, no2, so2, co) 
                VALUES ({year}, {month}, {day}, {hour}, {moe_station_id}, {data[city]["O3"]}, {data[city]["PM2.5"]}, {data[city]["NO2"]}, {data[city]["SO2"]}, {data[city]["CO"]})
            """
            self._psql.command(command, 'w')

    def _get_valid_time_range(self) -> List[datetime]:
        
        command = f"SELECT * FROM {Tables.MOE} ORDER BY year DESC, month DESC, day DESC, hour DESC LIMIT 1"
        row = self._psql.command(command, 'r')
        most_recent = datetime(row[0][2], row[0][3], row[0][4], row[0][5]) if row else datetime.now() - timedelta(hours=1) # returns current datetime if no entries yet
        
        now = datetime.now()
        hours = int(divmod((now - most_recent).total_seconds(), 3600)[0])
        time_range = [most_recent + timedelta(hours=i) for i in range(1, hours+1)]

        return time_range

    def _get_data(self, year: int, month: int, day: int, hour: int) -> Dict[str, Dict[str, float]]:

        data = {}
        column_order = {}

        # connect to webpage
        url = f"http://www.airqualityontario.com/history/summary.php?start_day={day}&start_month={month}&start_year={year}&my_hour={hour}"
        page = urlopen(url)
        html = page.read().decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")

        # find table
        table = soup.find("table")
        thead, tbody = table.thead, table.tbody

        # confirm order of data columns
        th = thead.find_all("th")
        for i in range(len(th)):
            if th[i].a: # ignore first column
                column_order[i] = th[i].a.text 

        # get data from columns
        tr = tbody.find_all("tr")

        for row in range(len(tr)):
            td = tr[row].find_all("td")
            city = td[0].div.text
            data[city] = {}

            for col in range(1, len(td)):
                data[city][column_order[col]] = "NULL" if (not td[col].div or not td[col].div.text.replace('.', '').isdigit()) else td[col].div.text

        return data



if __name__ == "__main__":

    HOST = "192.168.200.72"
    DATABASE = "postgres"
    USER = "socaar_reader"
    PASSWORD = "wallberg123"

    moe = MOE_Pollutant_Concentrations(HOST, DATABASE, USER, PASSWORD)
    moe.update_data()