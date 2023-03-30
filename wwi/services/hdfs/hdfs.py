from hdfs import InsecureClient

from wwi.dependencies.settings import HDFS_URL


class HDFSService:
    def __init__(self) -> None:
        self.client = InsecureClient(url=HDFS_URL)

    def is_exists(self, prefix: str):
        return True if self.client.status(prefix, strict=False) else False
