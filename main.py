import logging
import datetime
import time
import os
import psutil
from logquery.log_query import LogQuery

if __name__ == '__main__':
    process = psutil.Process(os.getpid())

    tic = time.perf_counter()
    lq = LogQuery(server1="server1.log", server2="/temp/server2.log", db_server="db_server.log")
    g = lq.query(start=datetime.datetime(2021, 1, 17, 14), entries=100, servers=["server1", "server2"],
                 min_severity=logging.WARN)
    result_size = len(list(g))
    toc = time.perf_counter()
    print(f"Initial query with {result_size} entries in {toc - tic:0.4f} seconds.")

    tic = time.perf_counter()
    g = lq.query(start=datetime.datetime(2021, 1, 17, 14, 30), entries=100, servers=["server1", "server2"],
                 min_severity=logging.CRITICAL)
    result_size = len(list(g))
    toc = time.perf_counter()

    print(f"Subsequent query with {result_size} entries in {toc - tic:0.4f} seconds")
