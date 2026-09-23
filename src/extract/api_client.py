import requests
from extract.base import Extractor

class JolicaClient(Extractor):
    def __init__(self):
        super().__init__(source_name="jolpica_api")
        self.source_name = "jolpica_api"
        self.baseURL = "https://api.jolpi.ca/ergast/f1"

    #Cao du lieu ve ket qua
    def extract_result(self, season :int, round: int):
        URL = f"{self.baseURL}/{season}/{round}/results.json"
        race_id = f"{season}_{round}"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data["MRData"]["RaceTable"]["Races"][0]["Results"]
        #To recognize the race each year and source system.
        for result in results:
            result["race_id"] = race_id
            result["source"] = self.source_name
        return results
    def extract_driver(self, season :int):
        URL = f"{self.baseURL}/{season}/drivers.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data["MRData"]["DriverTable"]["Drivers"]
        # Add source information
        for driver in results:
            driver["source"] = self.source_name
        return results

    def extract_race(self, season :int, round :int):
        URl = f"{self.baseURL}/{season}/{round}.json"
        race_id = f"{season}_{round}"
        response = requests.get(URl, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data["MRData"]["RaceTable"]["Races"]
        # Add source information
        for race in results:
            race["race_id"] = race_id 
            race["source"] = self.source_name  
        return results

    def extract_constructor(self, season :int):
        URL = f"{self.baseURL}/{season}/constructors.json"
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        results = data["MRData"]["ConstructorTable"]["Constructors"]
        # Add source information
        for constructor in results:
            constructor["source"] = self.source_name
        return results

    def extract(self, season: int, round: int):
        result = {}
        result["Race"] = self.extract_race(season, round)
        result["Driver"] = self.extract_driver(season)
        result["Constructor"] = self.extract_constructor(season)
        result["Results"] = self.extract_result(season, round)
        return result
        
    
