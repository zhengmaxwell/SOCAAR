from Postgres import Postgres
from Tables import Tables
from urllib.request import Request, urlopen
import requests
import zipfile
import shutil
from bs4 import BeautifulSoup
import os
from datetime import date, datetime
import pandas as pd
from openpyxl import load_workbook
from typing import Union, List, Dict, Tuple
import sys # TODO remove maybe



class NAPS_Pollutant_Concentrations():

    FIRST_YEAR = 1990
    LAST_YEAR = datetime.now().year - 1 # TODO: check this range
    

    def __init__(self, hostname, database, user, password) -> None:
        
        self._psql = Postgres(hostname, database, user, password)

        self._create_tables()


    def update_data(self) -> None:

        self._update_continuous_data()
        self._update_integrated_data()


    def _update_continuous_data(self, year: int=None) -> None:

        if year:
            self._get_data(year, 0)
        else:
            for year in self._get_valid_year_range(): # starts from first year
                self._get_data(year, 0)

    def _update_integrated_data(self, year: int=None) -> None:

        if year:
            self._get_data(year, 1)
        else:
            for year in self._get_valid_year_range()[::-1]: # starts last year and goes backwards
                self._get_data(year, 1)

    def _create_tables(self) -> None:

        Tables.connect(self._psql)
        Tables.create_naps_continuous()
        Tables.create_naps_integrated_carbonyls()
        Tables.create_naps_integrated_voc()

    def _get_data(self, year: int, dataTypeVal: int) -> None:

        # dataTypeVal = 1 -> IntegratedData
        # dataTypeVal = 0 -> continuousData

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
                        data = self._get_integrated_data(zip_dir)
                        self._insert_integrated_data(data)
                    else:
                        data = self._get_continuous_data(filepath) 
                        self._insert_continuous_data(data)
                    self.__delete_files(filepath, zip_dir)

    def _insert_continuous_data(self, data: List[Dict[str, str]]) -> None:

            pollutant = data[0]["Pollutant"] # should only have one pollutant per csvfile
            pollutant_id = Tables.get_naps_pollutants(pollutant)
            
            for line in data:
                naps_id = line["NAPSID"]
                naps_station_id = Tables.get_naps_station(naps_id)
                date = line["Date"]
                year, month, day = int(date[:4]), int(date[4:6]), int(date[6:])
                
                for hour in range(24):
                    timestamp = str(datetime(year, month, day, hour))
                    density = "NULL" if line[hour] is None else float(line[hour])
                    command = f"""
                        INSERT INTO {Tables.NAPS_CONTINUOUS} (timestamp, naps_station, pollutant, density)
                        VALUES (%(timestamp)s, {naps_station_id}, {pollutant_id}, {density})
                    """
                    str_params = {"timestamp": timestamp}
                    self._psql.command(command, 'w', str_params=str_params)
    
    def _get_continuous_data(self, filepath: str) -> List[Dict[str, str]]:
        
        column_order = {}
        total_data = []
        data_len = 31

        df = pd.read_csv(filepath, sep='\n', error_bad_lines=False)
        for row in range(len(df)):
            line = df.loc[row][0].split(',')
            if column_order or (len(line) == data_len and line.count('') == 0): # first row is column headers
                if not column_order:
                    for i in range(data_len):
                        if len(line[i].split("//")[0]) == 3 and line[i].split("//")[0][0] == 'H': # remove "H0" from hour
                            column_order[i] = int(line[i].split("//")[0].replace('H', '').replace('24', '0')) # TODO: is H24 midnight?
                        else:
                            column_order[i] = line[i].split("//")[0]
                
                else:
                    data = {}
                    for i in range(data_len):
                        data[column_order[i]] = line[i]
                    total_data.append(data)

        return total_data

    def _insert_integrated_data(self, data: Tuple[str, List[Dict[str, str]]]) -> None:

        pollutant, lines = data 

        if pollutant.upper() == "CARBONYLS":
            compounds = Tables.get_naps_integrated_all_pollutant_compounds(pollutant.upper())
            table = Tables.NAPS_INTEGRATED_CARBONYLYS
        elif pollutant.upper() == "VOC":
            compounds = Tables.get_naps_integrated_all_pollutant_compounds(pollutant.upper())
            table = Tables.NAPS_INTEGRATED_VOC
        
        for line in lines:
            naps_station_id = Tables.get_naps_station(int(line.pop("NAPS ID")))
            sample_type_id = Tables.get_naps_sample_type(line.pop("Sample Type"))
            year, month, day = [int(d) for d in line.pop("Sampling Date").split('-')]
            sampling_date = str(date(year, month, day))

            for c in compounds:
                compound = c[0]
                if compound in line.keys():
                    compound_id = Tables.get_naps_integrated_pollutant_compound(pollutant.upper(), compound)
                    density = "NULL" if line[compound] is None else float(line.pop(compound))
                    density_mdl = "NULL" if line[f"{compound}-MDL"] is None else float(line.pop(f"{compound}-MDL"))
                    vflag = line.pop(f"{compound}-VFlag") or "NULL"
                    vflag_id = Tables.get_naps_validation_code(vflag)

                command = f"""
                    INSERT INTO {table} (sampling_date, naps_station, sample_type, compound, density, density_mdl, vflag)
                    VALUES (%(sampling_date)s, {naps_station_id}, {sample_type_id}, {compound_id}, {density}, {density_mdl}, {vflag_id})
                """
                str_params = {"sampling_date": sampling_date}
                self._psql.command(command, 'w', str_params=str_params)


            # remaining compounds are duplicates with different metadata
            # TODO: try to combine this with above
            for compound in line.keys():
                if "MDL" not in compound and "VFlag" not in compound:
                    compound_name, metadata = compound.split("-metadata:")
                    compound_id = Tables.get_naps_integrated_pollutant_compound(pollutant.upper(), compound)
                    density = "NULL" if line[compound] is None else float(line[compound])
                    density_mdl_name = f"{compound_name}-MDL-metadata:{metadata}"
                    density_mdl = "NULL" if line[density_mdl_name] is None else float(line[density_mdl_name])
                    vflag_name = f"{compound_name}-VFlag-metadata:{metadata}"
                    vflag = line[vflag_name] or "NULL"
                    vflag_id = Tables.get_naps_validation_code(vflag)

                    command = f"""
                        INSERT INTO {table} (sampling_date, naps_station, sample_type, compound, density, density_mdl, vflag)
                        VALUES (%(sampling_date)s, {naps_station_id}, {sample_type_id}, {compound_id}, {density}, {density_mdl}, {vflag_id})
                    """
                    str_params = {"sampling_date": sampling_date}
                    self._psql.command(command, 'w', str_params=str_params)

    def _get_integrated_data(self, zip_dir: str) -> Tuple[str, List[Dict[str, str]]]:

        pollutant = zip_dir.split('\\')[-1].split('-')[0]
        
        if pollutant in ["CARBONYLS", "VOC"]:
            total_data = []
            for f in os.listdir(zip_dir):
                if f.split('.')[-1] == "xlsx" and f.split('.')[0].split('_')[-1] == "EN":
                    naps_id = f.split('.')[0].split('_')[0].replace('S', '')
                    xlsx_filepath = f"{zip_dir}/{f}"
                    print(xlsx_filepath)
                    wb = load_workbook(xlsx_filepath)
                    sheet = wb[pollutant]
                    
                    column_order = {} # col: name
                    meta_data_headers = ["Medium", "Observation Type", "Analytical Instrument", "Speciation Sampler Cartridge"]
                    meta_data_rows = {} # name: row
                    data_len = None

                    for row in range(1, sheet.max_row+1):
                        print(row) # TODO: remove
                        # find column headers
                        if not column_order:
                            # find metadata headers
                            if sheet[row][0].value in meta_data_headers:
                                meta_data_rows[sheet[row][0].value] = row
                            # strip Nonetypes from end of list and replace "None" strings in cells with "N/A"
                            # joining with '`' because ',' is included in some header strings
                            line = '`'.join(["None" if not cell.value else str(cell.value.replace("None", "N/A")) if True else 0 for cell in sheet[row]]).strip("`None").split('`') 
                            if line[0] and line.count("None") == 0:
                                data_len = len(line) 
                                i = 0
                                while i < data_len:
                                    # finding diplicate compound names with different metadata
                                    if line.count(sheet[row][i].value) > 1:
                                        meta_data = {}
                                        for meta_data_name in meta_data_rows.keys(): # only use meta_data that was found
                                                meta_data[meta_data_name] = sheet[meta_data_rows[meta_data_name]][i].value.replace('_', '/')
                                        for j in range(3):
                                            column_order[i+j] = f"{sheet[row][i+j].value}-metadata:{meta_data}"
                                        i += 3
                                    else:
                                        column_order[i] = sheet[row][i].value.replace("Sampling Type", "Sample Type")
                                        i += 1
                                
                        else:
                            data = {}
                            for i in range(data_len):
                                data[column_order[i]] = sheet[row][i].value
                            total_data.append(data)
                                
                    return pollutant, total_data

    def _get_valid_year_range(self) -> List[int]:

        # changes const value instead of function output in case it returns None
        start_year = self._get_most_recent_year() or NAPS_Pollutant_Concentrations.FIRST_YEAR-1 
        year_range = [year for year in range(start_year+1, NAPS_Pollutant_Concentrations.LAST_YEAR)]

        return year_range
        
    def _get_most_recent_year(self) -> Union[int, None]:

        cmd = f"""
            SELECT year FROM {Tables.NAPS_CONTINUOUS} ORDER BY year DESC LIMIT 1 
        """
        year = self._psql.command(cmd, 'r')

        return None if year == [] else year[0][0]

    def __download_file(self, url: str, filepath: str) -> Union[str, None]:

        # for whatever reason, the NAPS data doesn't doesn't download properly
        # tries multiple times to download until expected filesize matches downloaded size
        # TODO: possible infinite while loop
        download_size = -1
        size = int(requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, stream=True).headers["Content-Length"])
        
        while abs(download_size - size) > 1: # last byte of data isn't important I think ...
            headers = {"User-Agent": "Mozilla/5.0", "Range": f"bytes={download_size+1}-{size}"}
            with requests.get(url, headers=headers, stream=True, timeout=None) as r:
                with open(filepath, "ab") as f:
                    shutil.copyfileobj(r.raw, f, 1024)
                    f.flush()
                    os.fsync(f.fileno())
            download_size = os.path.getsize(filepath)
        
        # if file is a zip file, will unzip contents to the same directory and return directory name
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