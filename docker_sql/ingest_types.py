from abc import ABC


class IIngestDataParams(ABC):
    table: str
    file_link: str
