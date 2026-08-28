import requests
from extract.base import Extractor

class JolicaClient(Extractor):
    def __init__(self):
        super().__init__(source_name="jolica_api")
        self.baseURL = "https://api.jolpi.ca/ergast/f1"

    #Cao du lieu ve ket qua
    def extract_result(self, seasons :int, round: int):
        URL = f"{self.baseURL}/{seasons}/{round}/results.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["RaceTable"]["Races"][0]["Results"]

    def extract_driver(self, seasons :int):
        URL = f"{self.baseURL}/{seasons}/drivers.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["DriverTable"]["Drivers"]

    def extract_race(self, seasons :int, round :int):
        URl = f"{self.baseURL}/{seasons}/{round}.json"
        response = requests.get(URl, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["RaceTable"]["Races"]

    def extract(self, seasons: int, round: int):
        result = {}
        result["Race"] = self.extract_race(seasons, round)
        result["Driver"] = self.extract_driver(seasons)
        result["Results"] = self.extract_result(seasons, round)
        return result
        
    
