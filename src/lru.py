from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key):
        if key not in self.cache:
            return None
        else:
            # Move the recently accessed item to the end
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            # If key is in cache, delete it
            del self.cache[key]
        elif len(self.cache) == self.capacity:
            # If cache is at capacity, remove oldest item
            self.cache.popitem(last=False)

        # Add the key-value pair to the cache
        self.cache[key] = value

    def contains(self, key):
        return key in self.cache
    
    def __str__(self) -> str:
        return f"{self.cache.keys()}"