from abc import ABC
from ingest_types import IIngestDataParams


class IngestParams(IIngestDataParams):
    def __init__(self, args) -> None:
        self.table = args.table
        self.file_link = args.file_link
