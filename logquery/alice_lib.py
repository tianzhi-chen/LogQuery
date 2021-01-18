import logging
import random
import datetime
import string
import os


class AliceLib:
    """
    This dummy class pretends to use Alice's library to download file from remote servers.
    Instead, it just generate a log file locally.
    """
    line = 1000
    local_temp_dir = "temp/"
    random.seed(0)

    @staticmethod
    def get_remote_file(server, remote_file, local_output_file=None):
        """

        :param server: the name of the remote server
        :param remote_file: the remote file on the server to be downloaded
        :param local_output_file: optional, the path of the local output file
        :return: local_output_file
        """
        timestamp = datetime.datetime(2021, 1, 17, 12)
        if not local_output_file:
            local_output_file = AliceLib.local_temp_dir + remote_file

        # lazily download a file
        if not os.path.exists(local_output_file):
            local_file_dir = os.path.split(local_output_file)[0]
            os.makedirs(local_file_dir, exist_ok=True)
            with open(AliceLib.local_temp_dir + remote_file, 'w') as f:
                for _ in range(AliceLib.line):
                    timestamp += datetime.timedelta(seconds=random.randint(1, 30))
                    datetime_string = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    letters = string.ascii_lowercase
                    dummy_content = ' '.join(
                        [(''.join(random.choice(letters) for i in range(100))) for j in range(10)])
                    f.write(
                        f"[{datetime_string}][{random.choice(list(logging._nameToLevel.keys()))}] {dummy_content}\n")
        return local_output_file
