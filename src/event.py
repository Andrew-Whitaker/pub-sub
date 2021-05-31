# event.py>
from enum import Enum

# Constants for accessing data fields out of the Control Events'
# data dictionaries
CHORD_RING = "ring"
PREDECESSOR = "predecessor"
SEGMENT = "segment"

class EventType(Enum):
    PAUSE_OPER = 1
    RESUME_OPER = 2
    RESTART_BROKER = 3
    RING_UPDATE = 4
    UPDATE_TOPICS = 5
    VIEW_CHANGE = 6

class ControlEvent():
    def __init__(self, name: EventType, data=None) -> None:
        self.name = name # enum EventType
        if data == None:
            self.data = {} # events may have no helper data needed
        else:
            self.data = data # dictionary for data parameters
