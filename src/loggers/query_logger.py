"""
To define the file to log the queries in json form, change PATH_QUERY_LOG.

Use BATCH_SIZE if multiple file I/Os are of a concern. However,
if greater than 1, then the final entries might not be written (use QuerryLogger.flush() to ensure that they will).
"""

import json
import time
import os

QUERY_LOG_ENABLED = False
PATH_QUERY_DEFAULT_FOLDER = "./loggers/logs/query_logs/"
BATCH_SIZE = 100


class QueryLogger:
    _entries = []
    _ascending_id = 1
    _WG_FILE_LOGS = "wg_requests" + str(int(time.time())) + ".txt"

    @staticmethod
    def log_entry(label, time_query, query_json, time_reply, reply_json):
        """
        query and reply json should be in dictionary form!! not json string

        create manually one entry and add it to _entries
        """
        if QUERY_LOG_ENABLED:
            fields = dict()
            fields["label"] = label
            fields["time_start"] = time_query
            fields["query"] = query_json
            fields["time_end"] = time_reply
            fields["reply_json"] = reply_json
            QueryLogger._entries.append(fields)
            if len(QueryLogger._entries) >= BATCH_SIZE:
                QueryLogger._log_queries_json()

    @staticmethod
    def flush_entries():
        """
        save to the log file all class entries
        """
        return QueryLogger._log_queries_json()

    @staticmethod
    def _log_queries_json(path_log=PATH_QUERY_DEFAULT_FOLDER):
        """
        Writes the entries to folder.
        """
        if QUERY_LOG_ENABLED:
            # QueryLogger._write_json_array(path_log)
            QueryLogger._append_json(path_log)

    @staticmethod
    def _append_json(path_log):
        """ Check if file exists and append the entries (the resulted file is not valid json)"""
        p = path_log+QueryLogger._WG_FILE_LOGS
        try:

            with open(p, 'a') as f:
                for entry in QueryLogger._entries:
                    f.write("ENTRY: "+json.dumps(entry) + os.linesep)
                QueryLogger._entries = []
                return True
        except (OSError, IOError, Exception) as e:
            print("Error at query logger (writing file):")
            print(e)
            QueryLogger._entries = []  # even if the write isn't successful clear the entries
            return False


    @staticmethod
    def _write_json_array(path_log):
        try:
            # print("Writing Batch queries")

            p = path_log + QueryLogger._WG_FILE_LOGS
            print("Query logger path:")
            print(p)

            with open(p) as feedsjson:
                feeds = json.load(feedsjson)

            for entry in QueryLogger._entries:
                feeds.append(entry)

            with open(p, mode='w') as f:
                f.write(json.dumps(feeds, indent=2))

            QueryLogger._entries = []
            return True
        except (OSError, IOError, Exception) as e:
            print("Error at query logger (writing file):")
            print(e)
            QueryLogger._entries = []  # even if the write isn't successful clear the entries
            return False