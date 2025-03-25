import unittest
from uploader.DataToHDFS import HDFSClient

class TestHDFSClient(unittest.TestCase):
    def setUp(self):
        self.client = HDFSClient()

    def test_load_config(self):
        self.assertIn('namenode_host', self.client.config)
        self.assertIn('port', self.client.config)

    # Additional tests can be added for upload/download methods
    # For example, using a mock HDFS server or temporary files.

if __name__ == '__main__':
    unittest.main()