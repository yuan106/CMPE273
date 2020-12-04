import bisect
import hashlib

class ConsistentHashRing(object):
    # Implement a consistent hashing ring."""
    def __init__(self, nodes=None, replicas=3):
        # Create a new ConsistentHashRing.
        #`nodes` is a list of objects that have a proper __str__ representation.
        #`replicas` indicates how many virtual points should be used pr. node,
        #replicas are required to improve the distribution.
       
        self.replicas = replicas
        self._keys = []
        self._nodes = {}

        self.nodesByName = {}

    def _hash(self, key):
        """Given a string key, return a hash value."""

        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def _repl_iterator(self, nodename):
        """Given a node name, return an iterable of replica hashes."""

        return (self._hash("%s:%s" % (nodename, i))
                for i in range(self.replicas))

    def __setitem__(self, nodename, node):
        """Add a node, given its name.

        The given nodename is hashed
        among the number of replicas.

        """
        for hash_ in self._repl_iterator(nodename):
            if hash_ in self._nodes:
                raise ValueError("Node name %r is "
                            "already present" % nodename)
            self._nodes[hash_] = node
            bisect.insort(self._keys, hash_)

        self.nodesByName[nodename] = node

    def __delitem__(self, nodename):
        """Remove a node, given its name."""

        for hash_ in self._repl_iterator(nodename):
            # will raise KeyError for nonexistent node name
            del self._nodes[hash_]
            index = bisect.bisect_left(self._keys, hash_)
            del self._keys[index]

        del self.nodesByName[nodename]

    def __getitem__(self, key):
        """Return a node, given a key.

        The node replica with a hash value nearest
        but not less than that of the given
        name is returned.   If the hash of the
        given name is greater than the greatest
        hash, returns the lowest hashed node.

        """
        hash_ = self._hash(key)
        start = bisect.bisect(self._keys, hash_)
        if start == len(self._keys):
            start = 0
        return self._nodes[self._keys[start]]