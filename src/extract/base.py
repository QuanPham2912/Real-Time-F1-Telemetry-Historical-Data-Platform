from abc import ABC, abstractmethod
from datetime import datetime
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()

class Extractor(ABC):
    """
    Parent class for all source of data(api, statsF1, fastF1, ...)
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
    
    @abstractmethod
    def extract(self, **kwargs) -> dict[str, list[dict]]:
        """ Each child class defines its own way of extracting data from its respective source """
        raise NotImplementedError
    
    def run(self, **kwargs) -> dict[str, list[dict]]:
        """
        Log and error handling during extraction
        child classes will not override this method
        """
        logger.info(f"Starting extraction process: [{self.source_name}]")
        try:
            data = self.extract(**kwargs)
            logger.info(f"[{self.source_name}] Successfully extracted {len(data)} rows")
            return data
        except Exception as e:
            logger.exception(f"[{self.source_name}] Extraction failed: {e}")
            raise
