from urllib.request import Request, urlopen
import requests
import zipfile
import shutil
from bs4 import BeautifulSoup
import os
import csv
from typing import Union, List, Dict
import sys # TODO remove maybe



class National_Air_Pollution_Surveillance():

    def __init__(self):
        pass

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
                    print(filename)
                    file_url = url.split('?')[0] + filename
                    zip_dir = self.__download_file(file_url, filepath)
                    data = self._get_continous_data(filepath) # if not dataTypeVal else self._get_integrated_data(filepath)
                    # TODO upload data to database
                    self.__delete_files(filename, zip_dir)
                    sys.exit()

    
    def _get_continous_data(self, filepath: str) -> List[Dict[str, str]]:

        csv_file = open(filepath, 'r')
        csv_reader = csv.reader(csv_file)

        column_order = {}
        columns_ordered = False
        total_data = []
        data_len = 31
        
        for line in csv_reader:
            if len(line) == data_len: # only use relevant rows
                if not columns_ordered: # first row is column headers
                    for i in range(data_len):
                        column_order[i] = line[i].split("//")[0]
                    columns_ordered = True

                else: # data starts
                    data = {}
                    for i in range(data_len):
                        data[column_order[i]] = line[i] if line[i] != "-999" else None
                    total_data.append(data)

        return total_data
                    

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

    NAPS = National_Air_Pollution_Surveillance()
    NAPS._get_data(2019, 1)