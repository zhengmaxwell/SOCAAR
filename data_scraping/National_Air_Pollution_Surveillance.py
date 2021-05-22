from urllib.request import Request, urlopen
import requests
from bs4 import BeautifulSoup
import os


class National_Air_Pollution_Surveillance():

    def __init__(self):
        pass

    def _get_data(self, year: int, dataTypeVal: int) -> None:

        column_order = {} # {column: index}

        # connect to webpage
        dataType = "IntegratedData-DonneesPonctuelles" if dataTypeVal else "ContinuousData-DonneesContinu"
        url = f"https://data-donnees.ec.gc.ca/data/air/monitor/national-air-pollution-surveillance-naps-program/Data-Donnees/{year}/{dataType}/?lang=en"
        hdr = {"User-Agent": "Mozilla/5.0"}
        request = Request(url, headers=hdr)
        page = urlopen(request)
        html = page.read().decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")

        # confirm relevant column indices
        th = soup.find_all("th")
        for i in range(len(th)):
            if th[i].text in ("Name", "Description"):
                column_order[th[i].text] = i

        # get relevant rows of data
        tr = soup.find_all("tr")
        for i in range(len(tr)):
            if tr[i]["class"][0] != "indexhead": # ignore row from header
                td = tr[i].find_all("td")
                if td[column_order["Description"]].text == "Data archive":
                    filename = td[column_order["Name"]].text
                    file_url = url.split('?')[0] + filename
                    self._download_file(file_url, filename)
                    # TODO
                    self._delete_file(filename)
                    

    def _download_file(self, url: str, filename: str) -> None:

        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        open(filename, "wb").write(r.content)


    def _delete_file(self, filename: str) -> None:

        filepath = f"{ os.path.dirname(os.path.realpath(__file__))}/{filename}"
        if os.path.exists(filepath):
            os.remove(filepath)



