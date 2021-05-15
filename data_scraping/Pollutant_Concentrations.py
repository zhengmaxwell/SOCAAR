from urllib.request import urlopen
from bs4 import BeautifulSoup
import psycopg2
from typing import Dict

class Pollutant_Concentrations():

    def __init__(self, host: str, database: str, user: str, password: str):

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def _connect(self) -> None:

        try:
            conn = psycopg2.connect(
                host = self.host,
                database = self.database,
                user = self.user,
                password = self.password
            )

            self.conn = conn

        except:
            raise Exception("Failed to connect")

    def create_table(self) -> None:

        if not self.conn:
            self._connect()

        command = """
            CREATE TABLE socaar.POLLUTANT_CONCENTRATIONS(
                id SERIAL PRIMARY KEY,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT_TIMESTAMP,
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

        cur = self.conn.cursor()
        cur.execute(command)
        cur.close()
        conn.commit()


    def _get_data(self, day: int, month: int, year: int, hour: int) -> Dict[str, Dict[str, float]]:

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
                data[city][column_order[col]] = td[col].div.text if td[col].div else None

        return data



    




    
