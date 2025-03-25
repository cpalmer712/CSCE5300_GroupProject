import time
import logging
import socket
from hdfs.util import HdfsError
from DataToHDFS import HDFSClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def datanode_available(host="datanode", port=9864, retries=10, delay=5):
    for _ in range(retries):
        try:
            with socket.create_connection((host, port), timeout=3):
                return True
        except OSError:
            time.sleep(delay)
    return False

def main():
    if not datanode_available():
        logging.error("Datanode not reachable.")
        return

    client = HDFSClient(config_path="config.yml")
    try:
        client.upload_file("medicine_data.csv", "/user/hdfs/dataset/medicine_data.csv")
        logging.info("Upload complete.")
    except HdfsError as e:
        logging.error(f"Upload failed: {e}")

if __name__ == "__main__":
    main()
