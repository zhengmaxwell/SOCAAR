from urllib.request import urlopen
from bs4 import BeautifulSoup
from typing import Dict

class Pollutant_Concentrations():

    def __init__(self):
        pass


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



    




    
