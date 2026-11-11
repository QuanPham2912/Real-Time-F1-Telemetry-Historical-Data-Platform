from importlib import import_module
from extract.base import Extractor

SOURCES = {
    "jolpica": "extract.api_client:JolpicaClient",
    "fastf1": "extract.fastf1_extractor:FastF1Extractor",
    "statsf1": "extract.scraper:StatsF1",
}


def registry(name: str, path: str) -> None:
    SOURCES[name.lower()] = path


def available_sources() -> list[str]:
    return list(SOURCES)


def create_extractor(name: str) -> Extractor:
    try:
        module_name, class_name = SOURCES[name.lower()].split(":")
    except KeyError:
        raise ValueError(
            f"Unknown source '{name}'. Available: {available_sources()}"
        ) from None
    return getattr(import_module(module_name), class_name)()
