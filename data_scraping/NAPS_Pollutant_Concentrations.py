from Postgres import Postgres
from Tables import Tables
from urllib.request import Request, urlopen
import requests
import zipfile
import shutil
from bs4 import BeautifulSoup
import os
import csv
from datetime import datetime
from typing import Union, List, Dict
import sys # TODO remove maybe



class NAPS_Pollutant_Concentrations():

    FIRST_YEAR = 1990
    
    def __init__(self, hostname, database, user, password) -> None:
        
        self._psql = Postgres(hostname, database, user, password)
        self._naps_ids = {} # maps naps_id to naps_stations id

        self._create_tables()


    def update_data(self) -> None:

        for year in self._get_valid_year_range():
            self._get_data(year, 0) # continous data
            self._get_data(year, 1) # integrated data


    def _create_tables(self) -> None:

        Tables.create_naps_continuous(self._psql)

    def _get_data(self, year: int, dataTypeVal: int) -> None:

        # dataTypeVal = 1 -> IntegratedData
        # dataTypeVal = 0 -> ContinousData

        column_order = {} # {column: index}

        # connect to webpage
        dataType = "IntegratedData-DonneesPonctuelles" if dataTypeVal else "ContinuousData-DonneesContinu/HourlyData-DonneesHoraires"
        url = f"https://data-donnees.ec.gc.ca/data/air/monitor/national-air-pollution-surveillance-naps-program/Data-Donnees/{year}/{dataType}/?lang=en"
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
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
                description = "Data archive" if dataTypeVal else "Comma separated"

                if td[column_order["Description"]].text == description:
                    filename = td[column_order["Name"]].text
                    filepath = f"{os.path.dirname(os.path.realpath(__file__))}/{filename}"
                    print(filename) # TODO: remove
                    file_url = url.split('?')[0] + filename
                    zip_dir = self.__download_file(file_url, filepath)

                    if dataTypeVal:
                        data = self._get_integrated_data(filepath)
                        self._insert_integrated_data(data)
                    else:
                        data = self._get_continous_data(filepath) 
                        self._insert_continous_data(data)
                    self.__delete_files(filepath, zip_dir)

    def _insert_continous_data(self, data: List[Dict[str, str]]) -> None:

            pollutant = data[0]["Pollutant"] # should only have one pollutant per csvfile
            seen_naps_ids = []
            entryExists = False
            
            for line in data:
                naps_id = line["NAPSID"]
                date = line["Date"]
                year, month, day = int(date[:4]), int(date[4:6]), int(date[6:])

                # check if entry already exists
                # assumes if an entry exists for Jan 1 of the year for each distinct naps_id then an entry will exist 
                # for all remaining days of the year for that naps_id
                if naps_id not in seen_naps_ids:
                    if naps_id not in self._naps_ids:
                        command = f"""
                            SELECT id from {Tables.NAPS_STATIONS} WHERE naps_id = {naps_id}
                        """
                        self._naps_ids[naps_id] = self._psql.command(command, 'r')[0][0]

                    entryExists = False
                    command = f"""
                        SELECT COUNT(*) FROM {Tables.NAPS_CONTINUOUS}
                        WHERE naps_station = {self._naps_ids[naps_id]} AND year = {year} AND month = {month} AND day = {day}
                    """
                    entryExists = bool(self._psql.command(command, 'r')[0][0])
                    seen_naps_ids.append(naps_id)

                # insert data
                if not entryExists:
                    for hour in range(24):
                        command = f"""
                            INSERT INTO {Tables.NAPS_CONTINUOUS} (year, month, day, hour, naps_station, {pollutant})
                            VALUES ({year}, {month}, {day}, {hour}, {self._naps_ids[naps_id]}, {float(line[hour])})
                        """
                        self._psql.command(command, 'w')

                # update data
                else:
                    for hour in range(24):
                        command = f"""
                            UPDATE {Tables.NAPS_CONTINUOUS} SET {pollutant} = {float(line[hour])}
                            WHERE year = {year} AND month = {month} AND day = {day} AND hour = {hour} AND naps_station = {self._naps_ids[naps_id]}
                        """
                        self._psql.command(command, 'w')
    
    def _get_continous_data(self, filepath: str) -> List[Dict[str, str]]:
        
        column_order = {}
        columns_ordered = False
        total_data = []
        data_len = 31

        with open(filepath, 'r', encoding="utf-8", errors="ignore") as csv_file: # ignores non utf-8 encoded data
            csv_reader = csv.reader(csv_file)

            for line in csv_reader:
                if len(line) == data_len: # only use relevant rows
                    if not columns_ordered: # first row is column headers
                        for i in range(data_len):
                            if i in range(7, 31): # remove 'H0' from hour TODO: make the range dynamic
                                column_order[i] = int(line[i].split("//")[0].replace('H', '').replace('24', '0')) # TODO: should H24 be midnight
                            else:
                                column_order[i] = line[i].split("//")[0]
                        columns_ordered = True

                    else: # data starts
                        data = {}
                        for i in range(data_len):
                            data[column_order[i]] = line[i]
                        total_data.append(data)

        return total_data

    def _insert_integrated_data(self, data: List[Dict[str, str]]) -> None:
        pass

    def _get_integrated_data(self, filepath: str) -> List[Dict[str, str]]:
        pass

    def _get_valid_year_range(self) -> List[int]:

        # changes const value instead of function output in case it returns None
        start_year = self._get_most_recent_year() or NAPS_Pollutant_Concentrations.FIRST_YEAR-1 
        year_range = [year for year in range(start_year+1, datetime.now().year)] # up to last year

        return year_range
        
    def _get_most_recent_year(self) -> Union[int, None]:

        cmd = f"""
            SELECT year FROM {Tables.NAPS_CONTINUOUS} ORDER BY year DESC LIMIT 1 
        """
        year = self._psql.command(cmd, 'r')

        return None if year == [] else year[0][0]

    def __download_file(self, url: str, filepath: str) -> Union[str, None]:

        # if file is a zip file, will unzip contents to the same directory and return directory name

        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        open(filepath, "wb").write(r.content)

        if filepath.split('.')[-1] == "zip":
            return self.__unzip_file(filepath)
    
    def __unzip_file(self, filepath: str) -> str:

        # if file is a zip file, returns name of unzipped directory

        zip_file = zipfile.ZipFile(filepath, 'r')
        zip_name = [info.filename for info in zip_file.infolist() if info.is_dir()][0][:-1]
        zip_dir_path = f"{os.path.dirname(os.path.realpath(__file__))}/{zip_name}"
        zip_file.extractall(os.path.dirname(os.path.realpath(__file__)))
            
        return zip_dir_path

    def __delete_files(self, filepath: str, zip_dir_path: str) -> None:

        # deletes file and any existing unzipped directory

        if os.path.exists(filepath):
            os.remove(filepath)
            
        if zip_dir_path is not None and os.path.exists(zip_dir_path):
            shutil.rmtree(zip_dir_path)



if __name__ == "__main__":

    HOST = "192.168.200.72"
    DATABASE = "postgres"
    USER = "socaar_reader"
    PASSWORD = "wallberg123"

    naps = NAPS_Pollutant_Concentrations(HOST, DATABASE, USER, PASSWORD)
    naps.update_data()