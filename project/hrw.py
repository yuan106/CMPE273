# https://en.wikipedia.org/wiki/Rendezvous_hashing#cite_note-wrh-12


#!/usr/bin/env python3
import mmh3
import math

def int_to_float(value: int) -> float:
    """Converts a uniformly random [[64-bit computing|64-bit]] integer to uniformly random floating point number on interval <math>[0, 1)</math>."""
    fifty_three_ones = 0xFFFFFFFFFFFFFFFF >> (64 - 53)
    fifty_three_zeros = float(1 << 53)
    return (value & fifty_three_ones) / fifty_three_zeros

class Node(object):
    """Class representing a node that is assigned keys as part of a weighted rendezvous hash."""
    def __init__(self, node, seed, weight) -> None:
        self.node, self.seed, self.weight = node, seed, weight

    # def __str__(self):
    #     return "[" + self.name + " (" + str(self.seed) + ", " + str(self.weight) + ")]"

    def compute_weighted_score(self, key):
        hash_1, hash_2 = mmh3.hash64(str(key), 0xFFFFFFFF & self.seed)
        hash_f = int_to_float(hash_2)
        score = 1.0 / -math.log(hash_f)
        return self.weight * score

def determine_responsible_node(nodes, key):
    """Determines which node, of a set of nodes of various weights, is responsible for the provided key."""
    highest_score, champion = -1, None
    for node in nodes:
        score = node.compute_weighted_score(key)
        if score > highest_score:
            champion, highest_score = node, score
    return champion