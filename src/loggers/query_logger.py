"""
To define the file to log the queries in json form, change PATH_QUERY_LOG.

Use BATCH_SIZE if multiple file I/Os are of a concern. However,
if greater than 1, then the final entries might not be written (use QuerryLogger.flush() to ensure that they will).
"""

import json

QUERY_LOG_ENABLED = True
PATH_QUERY_FOLDER = "./loggers/logs/query_logs/"
BATCH_SIZE = 100


class QueryLogger:
    _entries = []
    _ascending_id = 1

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
    def _log_queries_json(path_log=PATH_QUERY_FOLDER):
        """
        Writes the entries to folder.
        """
        if QUERY_LOG_ENABLED:
            try:
                # print("Writing Batch queries")

                p = path_log + "web_gen_requests.json"
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
