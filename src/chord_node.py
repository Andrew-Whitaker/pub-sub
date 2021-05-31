# chordNode.py>
import hashlib
import logging

EXTRA_STR = "String to help push hashed values further from one another."
MAX_HASH = 2**30

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

def find_chord_successor(key: str, chord_ring, index: int = None):
    """Find the successor in the ring for the key.
    Parameters:
    - key: string to be hashed an sought after in the chord ring
    - chord_ring: array of ChordNode (assumed that this array is sorted by hash index)
    - index: optional value for when you know a Broker's index and can get a faster result

    Returns: tuple (ChordNode, index) chord node representing the successor and an
      int value for its index. Could return (None, -1) is the chord_ring is empty.
    """
    if len(chord_ring) == 0:
        return None, -1

    if index != None:
        succ_index = successor_index(index, len(chord_ring))
        return chord_ring[succ_index], succ_index

    # calculate hash of key
    id = chord_hash(key)

    # Slow Linear version of finding the Identifier
    for i in range(1, len(chord_ring)):
        # Current range is (chord_ring[i], chord_ring[i+1]]
        lower = chord_ring[i-1].hash_id
        upper = chord_ring[i].hash_id
        if id >= lower and id < upper:
            return chord_ring[i], i

    # Exiting the for loop means that the ID didn't belong
    # to any of the ranges we checked through. So it must belong
    # to Node[0] => either ID <= Node[0].ID OR ID > Node[last].ID
    return chord_ring[0], 0

def find_chord_predecessor(key: str, chord_ring, index: int = None):
    """Find the predecessor in the ring for the key.
    Parameters:
    - key: string to be hashed an sought after in the chord ring
    - chord_ring: array of ChordNode (assumed that this array is sorted by hash index)
    - index: optional value for when you know a Broker's index and can get a faster result

    Returns: tuple (ChordNode, index) chord node representing the predecessor and an
      int value for its index. Could return (None, -1) is the chord_ring is empty.
    """
    if len(chord_ring) == 0:
        return None, -1

    if index != None:
        pred_index = predecessor_index(index, len(chord_ring))
        return chord_ring[pred_index], pred_index

    # calculate hash of key
    id = chord_hash(key)

    # Slow Linear version of finding the Identifier
    for i in range(1, len(chord_ring)):
        # Current range is (chord_ring[i], chord_ring[i+1]]
        lower = chord_ring[i-1].hash_id
        upper = chord_ring[i].hash_id
        if id > lower and id <= upper:
            return chord_ring[i-1], i-1

    # Exiting the for loop means that the ID didn't belong
    # to any of the ranges we checked through. So it must belong
    # to Node[0] => either ID <= Node[0].ID OR ID > Node[last].ID
    last = len(chord_ring) - 1
    return chord_ring[last], last

def find_prime_chord_segment(address: str, chord_ring):
    """Find address tuple (start, end), both inclusive values, that represent
    the start and end of the primary responsibility segment of this address. 
    - Tip: address does not need to belong to an existing broker on the ring, but
    it may. 
    - If chord_ring is empty, then returned segment will be entire chord ring, 
    which is [0, MAX_HASH]
    """

    pred_node, pred_index = find_chord_predecessor(address, chord_ring)
    if pred_node == None:
        return 0, MAX_HASH
    start = pred_node.hash_id + 1
    end = chord_hash(address)
    return start, end

def find_repl_chord_segment(address: str, chord_ring):

    # pred1 is immediate predecessor of address
    pred1, pred1_index = find_chord_predecessor(address, chord_ring)
    # pred2 is the predeccesor of pred1
    pred2, pred2_index = find_chord_predecessor(pred1.key, chord_ring)

    repl_start = -1
    repl_end = -1

    if pred1.key != address:
        repl_start, repl_end = find_prime_chord_segment(pred1.key, chord_ring)
        if pred2.key != address:
            repl2_start, repl2_end = find_prime_chord_segment(pred2.key, chord_ring)
            repl_start = repl2_start

    return repl_start, repl_end


# Detect what part of the chord ring changed
# Return Boolean if your predecessor changed.
def check_if_new_leader(new_ring, old_ring, my_address):
    # determine indices of this Broker in both rings
    new_index = my_ring_index(new_ring, my_address)
    if new_index == -1:
        logging.warning("Addr {} was not found on the new chord ring".format(my_address))
        return False
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

def segment_range(start, end):
    if start == -1 and end == -1:
        return 0
    elif start <= end:
        return (end - start + 1)
    else:
        return (MAX_HASH - start) + (end + 1)

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

def successor_index(my_index, ring_length):
    return (my_index + 1) % ring_length

def customChordSort(node: ChordNode):
    return node.hash_id

# sortChordRing will modify your array in memory. It will not return
# a new array.
def sortChordRing(node_array):
    node_array.sort(key=customChordSort)
    return

