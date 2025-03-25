import logging
import yaml
from hdfs import InsecureClient

class HDFSClient:
    #Input local path for 'config.yml' file as the config_path variable
    def __init__(self, config_path='C:/Users/joeni/OneDrive/Documents/CSCE5300_GroupProject/config.yml'):
        self.config = self.load_config(config_path)
        self.client = InsecureClient(
            f"http://{self.config['namenode_host']}:{self.config['port']}",
            user=self.config.get('user', 'hdfs')
        )
        logging.info("HDFS Client initialized.")

    def load_config(self, config_path):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def upload_file(self, local_path, hdfs_path):
        logging.info(f"Uploading {local_path} to {hdfs_path} on HDFS")
        self.client.upload(hdfs_path, local_path)
        logging.info("Upload completed.")

    def download_file(self, hdfs_path, local_path):
        logging.info(f"Downloading {hdfs_path} to {local_path} from HDFS")
        self.client.download(hdfs_path, local_path, overwrite=True)
        logging.info("Download completed.")