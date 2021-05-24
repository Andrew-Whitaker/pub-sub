# chordNode.py>
import hashlib

EXTRA_STR = "String to help push hashed values further from one another."
MAX_HASH = 2**32

def chord_hash(key: str):
    hash_hex = hashlib.sha256(key.encode('utf-8')).hexdigest()
    return (int(hash_hex, 16) % MAX_HASH)

class ChordNode: 
    def __init__(self, key: str):
        self.key = key
        self.hash_id = chord_hash(key)

    def __str__(self) -> str:
        return 'Node(' + self.key + ',' + str(self.hash_id) + ')'

# Create a new chord ring from an array of broker addresses
def create_chord_ring(brokers):
    # build chord nodes from broker addresses
    chord_ring = []
    for address in brokers:
        node = ChordNode(address)
        chord_ring.append(node)
    sortChordRing(chord_ring)
    return chord_ring

def find_chord_successor(key: str, chord_ring):
    # calculate hash of key
    id = chord_hash(key)

    # Slow Linear version of finding the Identifier
    for i in range(len(chord_ring)):
        # Current range is (chord_ring[i], chord_ring[i+1]]
        lower = chord_ring[i-1].hash_id
        upper = chord_ring[i].hash_id
        if id > lower and id <= upper:
            return chord_ring[i]

    # Exiting the for loop means that the ID didn't belong
    # to any of the ranges we checked through. So it must belong
    # to Node[0] => either ID <= Node[0].ID OR ID > Node[last].ID
    return chord_ring[0]

# Detect what part of the chord ring changed
# Return Boolean if your predecessor changed.
def check_if_new_leader(new_ring, old_ring, my_address):
    # determine indices of this Broker in both rings
    new_index = my_ring_index(new_ring, my_address)
    if new_index == -1:
        raise ValueError("Addr {} was not found on the chord ring".format(my_address))
    old_index = my_ring_index(old_ring, my_address)
    if old_index == -1:
        return True

    # determine indices of predecessors on both rings
    new_pred = predecessor_index(new_index, len(new_ring))
    old_pred = predecessor_index(old_index, len(old_ring))

    if  new_ring[new_pred].key != old_ring[old_pred].key and new_index < old_index:
        return True
    else:
        return False
    

# Retrieve the Index of Broker in the Chord Ring
def my_ring_index(chord_ring, my_address):
    for i, node in chord_ring:
        if node.key == my_address:
            return i
    
    return -1

def predecessor_index(my_index, ring_length):
    if my_index > 0:
        return my_index - 1
    else:
        return ring_length - 1

def customChordSort(node: ChordNode):
    return node.hash_id

# sortChordRing will modify your array in memory. It will not return
# a new array.
def sortChordRing(node_array):
    node_array.sort(key=customChordSort)
    return

