import requests
from extract.base import Extractor
from metadata.f1_topic import F1Topic as T
from dataclasses import dataclass, field
from utils.config_reader import load_config


@dataclass(frozen=True)
class Endpoint:
    key: str
    path: str
    records_path: list
    tags: list = field(default_factory=lambda: ["source"])
    cache_per_season: bool = False


CONFIG_PATH = "extractors.yaml"
CONFIG = load_config(CONFIG_PATH)


class JolpicaClient(Extractor):
    topic_mapping = {
        "Race": T.JOLPICA_RACE,
        "Driver": T.JOLPICA_DRIVER,
        "Constructor": T.JOLPICA_CONSTRUCTOR,
        "Result": T.JOLPICA_RACE_RESULT,
    }

    def __init__(self):
        super().__init__(source_name="jolpica_api")
        self.source_name = "jolpica_api"
        self.baseURL = "https://api.jolpi.ca/ergast/f1"
        self.season_cache: dict[str, list[dict]] = {}

    def convert_endpoints(self, config: dict):
        return tuple(
            Endpoint(
                key=endpoint["key"],
                path=endpoint["path"],
                records_path=endpoint["records_path"],
                tags=endpoint.get("tags", ["source"]),
                cache_per_season=endpoint.get("cache_per_season", False),
            )
            for endpoint in config["api_client"]["endpoints"]
        )

    def fetch_page(self, URL: str):
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        return response.json()

    def lineage_tag(self, season: int, round: int):
        return {
            "race_id": f"{season}_{round}",
            "source": self.source_name,
            "season": season,
        }

    def fetch(self, endpoint: Endpoint, season: int, round: int = None):
        path = endpoint.path.format(season=season, round=round)
        URL = f"{self.baseURL}/{path}"
        # Check if the data for this season is already cached
        if endpoint.cache_per_season and season in self.season_cache:
            return self.season_cache[(endpoint.key ,season)]
        data = self.fetch_page(URL)["MRData"]
        for step in endpoint.records_path:
            data = data[step]

        tags = {
            k: v
            for k, v in self.lineage_tag(season, round).items()
            if k in endpoint.tags
        }
        for record in data:
            record.update(tags)

        if endpoint.cache_per_season:
            self.season_cache[(endpoint.key ,season)] = data
        return data

    def extract(self, season: int, round: int):
        return {
            ep.key: self.fetch(ep, season, round)
            for ep in self.convert_endpoints(CONFIG)
        }
