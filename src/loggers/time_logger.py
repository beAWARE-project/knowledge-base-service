"""
To disable the loggers set TIMING_ENABLED = False.

To define the file to log the execution times, change PATH_TIMING_LOG.

Use BATCH_SIZE if multiple file I/Os are of a concern. However,
if greater than 1 the final entries might not be stored (use TimeLogger.flush() to ensure that they will).
"""

import time

TIMING_ENABLED = False
PATH_TIMING_LOG = "./loggers/logs/time_logs.csv"
BATCH_SIZE = 1


class TimeLogger:
    incident_count = None  # the latest known incident count, might not be exactly correct, it has to be updated
    _entries = []

    @staticmethod
    def log_entry(func_name, start_time, end_time, tags):
        """
        create manually one entry and add it to _entries
        flush if necessary

        """
        fields = list()
        fields.append(str(func_name))
        fields.append(str(start_time))
        fields.append(str(end_time))
        fields.append(str(end_time - start_time))
        fields.append(str(TimeLogger.incident_count))
        for tag in tags:
            fields.append(str(tag))

        TimeLogger._entries.append(",".join(fields) + "\n")
        if len(TimeLogger._entries) >= BATCH_SIZE:
            TimeLogger._log_times_decorator()

    @staticmethod
    def flush_entries():
        """
        save to the log file all class _entries
        """
        return TimeLogger._log_times_decorator()

    @staticmethod
    def _log_times_decorator(path_log=PATH_TIMING_LOG):
        """
        Writes the entries to the file.
        """
        try:
            with open(path_log, "a") as f:
                for line in TimeLogger._entries:
                    f.write(line)
                # print("write to " + path_log)
            TimeLogger._entries = []
            return True
        except (OSError, IOError) as e:
            print("Error at loggers decorator (writing file):")
            print(e)
            TimeLogger._entries = []  # even if the write wasn't successful clear the entries
            return False

    @staticmethod
    def timer_decorator(tags=None):
        """
        In short it encapsulates the method with time measurements.
        """

        def intermediate_timer_decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    if TIMING_ENABLED:
                        start_time = time.time()
                        res = func(*args, **kwargs)
                        end_time = time.time()

                        TimeLogger.log_entry(func_name=func.__name__, start_time=start_time, end_time=end_time,
                                             tags=tags)

                        return res
                    else:
                        return func(*args, **kwargs)
                except Exception as e:
                    print("Error at timer decorator:")
                    print(e)

            return wrapper

        return intermediate_timer_decorator
