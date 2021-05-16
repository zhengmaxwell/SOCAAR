from urllib.request import urlopen
from bs4 import BeautifulSoup
import psycopg2
from datetime import timedelta, datetime
import sys
from typing import Dict, List, Union



class Pollutant_Concentrations():

    def __init__(self, host: str, database: str, user: str, password: str, table: str) -> None:

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.table = table

        self._connect()
        self._create_table()


    def insert_data(self, year: int, month: int, day: int, hour: int) -> None:

        command = f"SELECT COUNT(*) FROM {self.table} WHERE year = {year} AND month = {month} AND day = {day} AND hour = {hour}"
        count = self.command(command, 'r')[0][0]

        if count == 0: # check if data already exists
            data = self._get_data(year, month, day, hour)

            for city in data:
                command = f"""
                    INSERT INTO {self.table} (year, month, day, hour, city, o3, pm2_5, no2, so2, co) 
                    VALUES ({year}, {month}, {day}, {hour}, '{city}', {data[city]["O3"]}, {data[city]["PM2.5"]}, {data[city]["NO2"]}, {data[city]["SO2"]}, {data[city]["CO"]})
                """

                self._command(command, 'w')


    def _connect(self) -> None:

        try:
            conn = psycopg2.connect(
                host = self.host,
                database = self.database,
                user = self.user,
                password = self.password
            )

            self.conn = conn

        except Exception as e:
            print(e)
            sys.exit()


    def _create_table(self) -> None:

        command = "SELECT * FROM pg_catalog.pg_tables"
        rows = self.command(command, 'r')
 
        if self.table not in [row[1] for row in rows]: # only create if does not exist yet

            command = f"""
                CREATE TABLE {self.table}(
                    id SERIAL PRIMARY KEY,
                    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    year INTEGER NOT NULL CHECK (year <= DATE_PART('year', now())),
                    month INTEGER NOT NULL CHECK (month >= 1 and month <= 12),
                    day INTEGER NOT NULL CHECK (day >= 1 and day <= 31),
                    hour INTEGER NOT NULL CHECK (hour >= 0 and hour <= 23),
                    city VARCHAR NOT NULL,
                    o3 FLOAT,
                    pm2_b FLOAT,
                    no2 FLOAT,
                    so2 FLOAT,
                    co FLOAT
                )
            """

            self.command(command)
            
            
    def _command(self, command: str, mode: str) -> Union[None, str]:

        if mode not in ('r', 'w'): # read and write
            print("ERROR: mode parameter must be 'r' or 'w'")
            sys.exit()

        cur = self.conn.cursor()

        try:
            cur.execute(command)
        except Exception as e:
            print(e)
            sys.exit()
        
        if mode == 'r':
            rows = cur.fetchall()
            cur.close()
            
            return rows

        else:
            cur.close()
            self.conn.commit()


    def _get_valid_time_range(self) -> List[datetime]:
        
        command = f"SELECT * FROM {self.table} ORDER BY year DESC, month DESC, day DESC, hour DESC LIMIT 1"
        row = self.command(command, 'r')
        most_recent = datetime(row[0][2], row[0][3], row[0][4], row[0][5]) if row else datetime.now() # returns current datetime if no entries yet
        
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
                data[city][column_order[col]] = 0 if (not td[col].div or not td[col].div.text.replace('.', '').isdigit()) else td[col].div.text

        return data