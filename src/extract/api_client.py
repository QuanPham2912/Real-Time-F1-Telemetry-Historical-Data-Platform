import requests
from extract.base import Extractor

class JolicaClient(Extractor):
    def __init__(self):
        super().__init__(source_name="jolica_api")
        self.baseURL = "https://api.jolpi.ca/ergast/f1"

    #Cao du lieu ve ket qua
    def extract_result(self, season :int, round: int):
        URL = f"{self.baseURL}/{season}/{round}/results.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["RaceTable"]["Races"][0]["Results"]

    def extract_driver(self, season :int):
        URL = f"{self.baseURL}/{season}/drivers.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["DriverTable"]["Drivers"]

    def extract_race(self, season :int, round :int):
        URl = f"{self.baseURL}/{season}/{round}.json"
        response = requests.get(URl, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["RaceTable"]["Races"]
    
    def extract_constructor(self, season :int):
        URL = f"{self.baseURL}/{season}/constructors.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data["MRData"]["ConstructorTable"]["Constructors"]

    def extract(self, season: int, round: int):
        result = {}
        result["Race"] = self.extract_race(season, round)
        result["Driver"] = self.extract_driver(season)
        result["Constructor"] = self.extract_constructor(season)
        result["Results"] = self.extract_result(season, round)
        return result
        
    
