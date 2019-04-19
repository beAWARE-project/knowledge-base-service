"""
To define the file to log the queries in json form, change PATH_QUERY_LOG.

Use BATCH_SIZE if multiple file I/Os are of a concern. However,
if greater than 1 the final entries might not be stored (use QuerryLogger.flush() to ensure that they will).
"""

import json
import time

PATH_QUERY_FOLDER = "./loggers/logs/query_logs/"
BATCH_SIZE = 1


class QueryLogger:
    _entries = []
    _ascending_id = 1

    @staticmethod
    def log_entry(label, time_query, query_json, time_reply, reply_json):
        """
        query and reply json should be in dictionary form!! not json string

        create manually one entry and add it to _entries
        """

        fields = dict()
        fields["label"] = label
        fields["time_query"] = time_query
        fields["query_json"] = query_json
        fields["time_reply"] = time_reply
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
    def _log_queries_json(path_log=PATH_QUERY_FOLDER):
        """
        Writes the entries to folder.
        """
        try:
            # print("Writing Batch queries")
            for entry in QueryLogger._entries:
                p = path_log + entry["label"] + "_" + str(time.time()) + "_" + str(QueryLogger._ascending_id) + ".json"
                with open(p, "w+") as f:
                    f.write(json.dumps(entry))
                    # print("log json to file:" + p)
                    QueryLogger._ascending_id += 1

            QueryLogger._entries = []
            return True
        except (OSError, IOError, Exception) as e:
            print("Error at query logger (writing json file):")
            print(e)
            return False