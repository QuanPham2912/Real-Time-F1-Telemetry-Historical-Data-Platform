from enum import Enum

class DataIngestionType(str, Enum):
    BATCH_INGESTION = "batch_ingestion"
    STREAMING_INGESTION = "streaming_ingestion"
    def __str__(self):
        return self.value