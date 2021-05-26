# event.py>
from enum import Enum

class EventType(Enum):
    PAUSE_OPER = 1
    RESUME_OPER = 2
    RESTART_BROKER = 3
    UPDATE_TOPICS = 4
    VIEW_CHANGE = 5

class ControlEvent():
    def __init__(self, name: EventType, data=None) -> None:
        self.name = name # enum EventType
        if data == None:
            self.data = {} # events may have no helper data needed
        else:
            self.data = data # dictionary for data parameters
