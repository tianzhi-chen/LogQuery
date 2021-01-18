import datetime
import functools
import heapq
import logging
from sortedcontainers import SortedList
import re
import linecache
from logquery.alice_lib import AliceLib


class LogQuery:

    def __init__(self, **kwargs):
        """

        :param kwargs: a dict of remote files to search from.
            e.g: {server1: "/var/log/server1.log", "server12":"/temp/log.txt"}
        """
        self._server_to_remote_file = kwargs
        self._server_to_local_file = {}

        self._server_index: dict[str, SortedList[int, int]] = dict()
        # server index to quickly locate which lines belong to a server. It can support quick search on timestamp
        # leveraging the sorting feature of SortedList
        # dict[server name, SortedList[(epoch, line_number) ]]

        self._severity_index: dict[int, SortedList[(int, str, int)]] = dict()
        # severity index to quickly locate which lines belong to a severity. It can support quick search on timestamp
        # leveraging the sorting feature of SortedList
        # dict[severity, SortedList[(epoch, file_name, line_number) ]]

    @staticmethod
    def _to_epoch_seconds(dt: datetime):
        return int((dt - datetime.datetime(1970, 1, 1)).total_seconds())

    @staticmethod
    def _parse_line(line):

        m = re.match(r'\[(.*)\]\[(.*)\](.*)', line)
        datetime_string = m.group(1)
        severity_string = m.group(2)
        content = m.group(3)
        return datetime_string, severity_string, content

    @staticmethod
    def _get_line_metadata(line):

        datetime_string, severity_string, _ = LogQuery._parse_line(line)
        severity = getattr(logging, severity_string.upper())
        dt = datetime.datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
        epoch_seconds = LogQuery._to_epoch_seconds(dt)
        return epoch_seconds, severity

    def _add_to_server_index(self, epoch, server, line_number):
        self._server_index[server].add((epoch, line_number))

    def _add_to_severity_index(self, epoch, severity, server, line_number):
        if severity not in self._severity_index:
            self._severity_index[severity] = SortedList(key=lambda x: x[0])
        self._severity_index[severity].add((epoch, server, line_number))

    def _add_to_index(self, server, file_name):
        if server not in self._server_index:
            self._server_index[server] = SortedList(key=lambda x: x[0])
        with open(file_name, 'r') as f:
            line_number = 1
            for line in f:
                epoch, severity = self._get_line_metadata(line)
                self._add_to_server_index(epoch, server, line_number)
                self._add_to_severity_index(epoch, severity, server, line_number)
                line_number += 1

    def _add_server_log_to_index(self, server):
        file_name = AliceLib.get_remote_file(server, self._server_to_remote_file[server])
        self._server_index[server] = SortedList(key=lambda x: x[0])
        self._add_to_index(server, file_name)
        self._server_to_local_file[server] = file_name

    def search(self, servers, min_severity, start_epoch, entries):
        """
        This function first locate the first line in each server log that has a timestamp greater
        than or equal to the user specified start time. Then it leverage a heap to process line by line,
        ordered by timestamp, from ALL the server log. For each line indexed, this function looks up in the severity
        index to check if it meet the severity requirement, it only adds the line,
        represented by (timestamp, server, line_number) to the result list.

        Assuming that there are n servers, m entries in each server on average and a samll number of w severity levels.

        Locating the first line in each server will be O(logm), since we are using a sorted list to store all the lines,
        so we can take advantage of binary search.

        For each line, we need pop it from the heap, costing O(1), checking if it exists in proper severity indexes,
        costing O(log(nm/w)), then add the next line to heap, costing O(logn).

        Assuming a factor sparsity = (# of all the logs) / (# of log entries worse than min_severity)

        we need to process O(entries * sparsity * (1 + log(n) + w * log(nm/w))). If we assume m > n, then it can be
        simplified as O( entries * sparsity * log(m)).

        Worst case scenario, entries * sparsity = all the log lines indexed = n * m.


        :param servers: query parameter specified by the user
        :param min_severity: query parameter specified by the user
        :param start_epoch: query parameter specified by the user
        :param entries: query parameter specified by the user
        :return: the first #entries log indexes, (server, line_number) to get the raw log entry, sorted by timestamp

        """
        heap = []
        heapq.heapify(heap)
        result = []
        # add the first line from each server log that has a timestamp later than start_epoch and add to a heap
        for server in servers:
            idx = self._server_index[server].bisect_left((start_epoch, 0))
            timestamp, line_number = self._server_index[server][idx]
            heapq.heappush(heap, (timestamp, server, line_number, idx))

        while heap:
            curr_timestamp, server, curr_line_number, curr_idx = heapq.heappop(heap)
            # we always pop the line with the smallest timestamp from all the servers
            for severity in self._severity_index:
                if severity >= min_severity \
                        and (curr_timestamp, server, curr_line_number) in self._severity_index[severity]:
                    result.append((curr_timestamp, server, curr_line_number))
                    # only add to result list if it meets severity criterion too
                    if len(result) >= entries:
                        return result
            next_idx = curr_idx + 1
            if next_idx < len(self._server_index[server]):
                next_timestamp, next_line_number = self._server_index[server][next_idx]
                heapq.heappush(heap, (next_timestamp, server, next_line_number, next_idx))
                # add the next line in the server log to the heap

        return result

    @staticmethod
    def _search_results_intersection(*matched_lines, entries):
        matched_lines_set = [set(lines) for lines in matched_lines]

        final_result_set = functools.reduce(lambda a, b: a.intersection(b), matched_lines_set)

        return SortedList(final_result_set, key=lambda x: x[0])[:entries]

    def query(self, start: datetime.datetime, entries: int, servers: list[str], min_severity: int):
        for server in servers:
            # lazily index a log file
            if server not in self._server_to_local_file:
                self._add_server_log_to_index(server)

        start_epoch = self._to_epoch_seconds(start)
        matched_lines = self.search(servers, min_severity, start_epoch, entries)
        result_lines = matched_lines
        for timestamp, server, line_number in result_lines:
            file_name = self._server_to_local_file[server]
            raw_log_line = linecache.getline(file_name, line_number)
            datetime_string, severity_string, content = self._parse_line(raw_log_line)
            yield f"[{datetime_string}][{severity_string}][{server}] {content}"
