from hdfs import InsecureClient

from wwi.dependencies.settings import HDFS_URL


class BaseHDFS:
    def __init__(self) -> None:
        self.client = InsecureClient(url=HDFS_URL)

    def is_exists(self, path: str):
        raise NotImplementedError
