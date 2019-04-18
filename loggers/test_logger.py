from loggers.query_logger import QueryLogger as ql
from loggers.time_logger import TimeLogger
import json
import time


if False:
    with open("./logs/query_logs/abox_select_1555591973.5494516_3.json", "r") as f:
        j = json.load(f)
        print("cred:" + json.dumps(j))

    print(j)
    print(j["label"])

    q = j["query_json"]
    print(q)
    print(type(q))
    print(q['api_key'])
    # q = json.loads(q)
    # print(q['api_key'])

    exit()


else:

    with open("../dummy_credentials/bus_credentials.json", "r") as f:
        j = json.load(f)
        print("cred:" + json.dumps(j))

    for i in range(10):
        ql.log_entry(label="abox_select", time_query=time.time(), query_json=j, time_reply=time.time(), reply_json=j)

    ql.flush_entries()

    exit()


f1()
C().f2(a="a")
C.f3("b")

test_time.CC().f4()
TimeLogger.incident_count = 3
TimeLogger.flush_entries()

for i in range(5):
    f1()

TimeLogger.log_entry("fakeName", 12, 17, ["realTag1", "rt2"])
TimeLogger.flush_entries()

print("start parallel")
# time.sleep(10)
print("end parallel")
