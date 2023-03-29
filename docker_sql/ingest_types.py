from abc import ABC


class IIngestDataParams(ABC):
    user: str
    password: str
    port: str
    database: str
    table: str
    host: str
    file_link: str
