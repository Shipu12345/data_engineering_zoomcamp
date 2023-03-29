from abc import ABC
from ingest_types import IIngestDataParams


class IngestParams(IIngestDataParams):
    def __init__(self, args) -> None:
        self.user = args.user
        self.password = args.password
        self.port = args.port
        self.database = args.database
        self.table = args.table
        self.host = args.host
        self.file_link = args.file_link
