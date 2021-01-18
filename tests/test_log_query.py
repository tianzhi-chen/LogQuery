import logging
import os
import re
import unittest
import shutil
from datetime import datetime
from unittest import mock
from logquery import alice_lib
from logquery.alice_lib import AliceLib
from logquery.log_query import LogQuery


class TestLogQuery(unittest.TestCase):

    def setUp(self) -> None:
        temp_dir = alice_lib.AliceLib.local_temp_dir
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def _parse_result_line(self, line):

        m = re.match(r'\[(.*)\]\[(.*)\]\[(.*)\](.*)', line)
        datetime_str = m.group(1)
        severity_str = m.group(2)
        server_str = m.group(3)
        content = m.group(4)
        return datetime_str, server_str, severity_str, content

    def tearDown(self) -> None:

        temp_dir = alice_lib.AliceLib.local_temp_dir
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_result_size(self):
        entries = 50
        log_query = LogQuery(server1="/var/log/server1.log", db_server="temp.log")
        result = log_query.query(servers=["server1", "db_server"],
                                 min_severity=logging.WARN,
                                 start=datetime(2021, 1, 17, 15),
                                 entries=entries)
        result_list = list(result)
        self.assertTrue(len(result_list) <= entries, "query returned more entries than specified.")

    def test_severity(self):
        entries = 50
        warning_and_worse = ['WARN', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL']
        log_query = LogQuery(server1="severity_test/server1.log", db_server="severity_test/temp.log")
        result = log_query.query(servers=["server1", "db_server"],
                                 min_severity=logging.WARN,
                                 start=datetime(2021, 1, 17, 15),
                                 entries=entries)

        for entry in result:
            datetime_str, server_str, severity_str, content = self._parse_result_line(entry)
            self.assertTrue(severity_str.upper() in warning_and_worse,
                            f"query returned entries that do not meet severity requirement: {severity_str.upper()}")

    def test_result_is_ordered(self):
        entries = 50
        log_query = LogQuery(server1="order_test/server1.log", db_server="order_test/temp.log")
        result = log_query.query(servers=["server1", "db_server"],
                                 min_severity=logging.WARN,
                                 start=datetime(2021, 1, 17, 15),
                                 entries=entries)
        result_list = [self._parse_result_line(l) for l in list(result)]

        for idx in range(1, len(result_list)):
            self.assertTrue(result_list[idx][0] >= result_list[idx - 1][0],
                            f"query returned entries that are not in ascending order of timestamp.")

    def test_server(self):

        entries = 50
        servers = ["server1", "db_server"]
        log_query = LogQuery(server1="/var/log/server1.log", db_server="temp.log", server2="server2.txt")
        result = log_query.query(servers=servers,
                                 min_severity=logging.WARN,
                                 start=datetime(2021, 1, 17, 15),
                                 entries=entries)

        for entry in result:
            datetime_str, server_str, severity_str, content = self._parse_result_line(entry)
            self.assertTrue(server_str in servers,
                            f"query returned entries that do not meet server requirement: {server_str.upper()}")

    def test_timestamp(self):
        entries = 50
        start = datetime(2021, 1, 17, 15)
        servers = ["server1", "db_server"]
        log_query = LogQuery(server1="timestamp_test/server1.log",
                             db_server="timestamp_test/temp.log",
                             server2="timestamp_test/server2.txt")
        result = log_query.query(servers=servers,
                                 min_severity=logging.WARN,
                                 start=datetime(2021, 1, 17, 15),
                                 entries=entries)

        for entry in result:
            datetime_str, server_str, severity_str, content = self._parse_result_line(entry)
            self.assertTrue(datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S") >= start,
                            f"query returned entries that do not meet start time requirement: {datetime_str}")

    def test_lazy_index(self):

        entries = 50
        start = datetime(2021, 1, 17, 15)
        servers = ["server1"]
        log_query = LogQuery(server1="lazy_index/server1.log",
                             db_server="lazy_index/temp.log",
                             server2="lazy_index/server2.txt")

        with mock.patch.object(log_query, '_add_server_log_to_index',
                               wraps=log_query._add_server_log_to_index) as monkey:
            list(log_query.query(servers=servers,
                                 min_severity=logging.WARN,
                                 start=start,
                                 entries=entries))

            list(log_query.query(servers=servers,
                                 min_severity=logging.CRITICAL,
                                 start=start,
                                 entries=entries))
            monkey.assert_called_once_with("server1")

    def test_lazy_download(self):
        entries = 50
        start = datetime(2021, 1, 17, 15)
        servers = ["db_server"]

        with mock.patch.object(AliceLib, 'get_remote_file',
                               wraps=AliceLib.get_remote_file) as monkey:
            log_query = LogQuery(server1="lazy_download/server1.log",
                                 db_server="lazy_download/temp.log",
                                 server2="lazy_download/server2.txt")

            monkey.assert_not_called()

            list(log_query.query(servers=servers,
                                 min_severity=logging.WARN,
                                 start=start,
                                 entries=entries))

            list(log_query.query(servers=servers,
                                 min_severity=logging.CRITICAL,
                                 start=start,
                                 entries=entries))

            monkey.assert_called_once_with("db_server", "lazy_download/temp.log")


if __name__ == '__main__':
    unittest.main()
