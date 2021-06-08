import json
from json import JSONEncoder
import datetime as dt


class ThroughputStat():
    def __init__(self, start, end, msg_count, topic) -> None:
        self.topic = topic
        self.start_time = start
        self.end_time = end
        self.msg_count = msg_count

def encode_datetime(date_time):
    return date_time.isoformat()

def decode_datetime(dt_str):
    datetime_format = "%Y-%m-%dT%H:%M:%S"
    return dt.datetime.strptime(dt_str, datetime_format)

# class EncodeThroughput(JSONEncoder):
#     def default(self, obj):
#         print(type(obj))
#         if isinstance(obj, dt.datetime):
#             return obj.isoformat()
#         return JSONEncoder.default(self, obj)

# def StatEncoder(obj):
#     if isinstance(obj, dt.datetime):
#         return obj.isoformat()

# def StatDecoder(statDict):
#     datetime_format = "%Y-%m-%dT%H:%M:%S"
#     start = dt.datetime.strptime(statDict["start"], datetime_format)
#     end = dt.datetime.strptime(statDict["end"], datetime_format)
#     return ThroughputStat(start, end, statDict["msg_count"], statDict["topic"])


