import json
from json import JSONEncoder
import datetime as dt
import time

from pubsubClient import PubSubClient

STATS_TIME_FRAME = 1 # seconds
STATS_TOPIC = "SYSTEM_STATS"

class ThroughputStat():
    def __init__(self, start, end, msg_count, topic) -> None:
        self.topic = topic
        self.start_time = start
        self.end_time = end
        self.msg_count = msg_count

class StatEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__

def StatDecoder(statDict):
    return ThroughputStat(statDict["start_time"], statDict["end_time"], 
                        statDict["msg_count"], statDict["topic"])

def encode_datetime(date_time):
    return date_time.replace(microsecond=0).isoformat()

def decode_datetime(dt_str):
    datetime_format = "%Y-%m-%dT%H:%M:%S"
    return dt.datetime.strptime(dt_str, datetime_format)

class TopicEventLog():
    def __init__(self, start_time: dt.datetime) -> None:
        self.start_time = start_time
        self.timeline = []

    def add_event(self, event: ThroughputStat):
        event_start = decode_datetime(event.start_time)
        if event_start < self.start_time:
            return # outside boundary

        # determine index of array that it will fall in
        delta = event_start - self.start_time
        secs_since_start = delta.seconds 

        # Extend timeline array to meet data requirements
        if len(self.timeline) <= secs_since_start:
            ext = [0] * (secs_since_start - len(self.timeline) + 1)
            self.timeline.extend(ext)
        
        # Add data
        self.timeline[secs_since_start] += event.msg_count
        return

    def print_report(self):
        print("Time, Msgs/s")
        for i in range(len(self.timeline)):
            print("{}, {}".format(i, self.timeline[i]))


def run_stats_collection(simulation_start: dt.datetime, duration: int, psclient: PubSubClient):
    msg_id = 0
    simulation_start = simulation_start.replace(microsecond=0)
    end_time = simulation_start + dt.timedelta(seconds=duration)
    event_log = TopicEventLog(simulation_start)
    while dt.datetime.utcnow() < end_time:
        messages = psclient.consume(STATS_TOPIC, msg_id)
        for msg in messages:
            stat_event = json.loads(msg, object_hook=StatDecoder)
            event_log.add_event(stat_event)
            msg_id += 1
        time.sleep(0.5)

    event_log.print_report()



# class EncodeThroughput(JSONEncoder):
#     def default(self, obj):
#         print(type(obj))
#         if isinstance(obj, dt.datetime):
#             return obj.isoformat()
#         return JSONEncoder.default(self, obj)

# def StatEncoder(obj):
#     if isinstance(obj, dt.datetime):
#         return obj.isoformat()




