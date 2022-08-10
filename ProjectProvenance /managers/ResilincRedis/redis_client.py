#
# Copyright (c) 2019 Resilinc, Inc., All Rights Reserved.
# Any use, modification, or distribution is prohibited
# without prior written consent from Resilinc, Inc.
#

# stdlib Imports.

# Third Party Imports.
from redis import StrictRedis

# Local Imports.


class RedisClient(StrictRedis):

    # All the methods provided by Strict Redis are Sufficient, only override methods which don't provide setting ttl.

    def hmset(self, name: str, mapping: dict, ttl: int = None) -> bool:
        """
        Set a HashMap to Redis with Time to Live.

        :param name: Name of the Map.
        :type name: str
        :param mapping: Map to be set against name.
        :type mapping: dict
        :param ttl: Time to Live in Seconds to set for the hash. 0/negative/None indicate infinite time to live.
        :return: bool
        """

        super(RedisClient, self).hmset(name=name, mapping=mapping)
        if ttl and ttl > 0:
            super(RedisClient, self).expire(name=name, time=ttl)
        return True

    def hset(self, name, key, value, ttl: int = -1):
        """
        Set a Hash to Redis, with Time to Live.

        :param name: Name of Hash to set.
        :param key: Key of Hash to Set
        :param value: Value of Has to set
        :param ttl: Time to Live in Seconds to set for the hash. 0/negative/None indicate infinite time to live.
        :return: None
        """

        super(RedisClient, self).hset(name=name, key=key, value=value)
        if ttl and ttl > 0:
            super(RedisClient, self).expire(name=name, time=ttl)

    def getset(self, name, value, new_ttl=None):
        """
        Sets the value at key name to value and returns the old value at key name atomically.

        :param name: Name of Key
        :param value: Value
        :param new_ttl: New Time to Live for new Name-Value Pair, if None then ttl of previous Key will be set.
        :return:
        """

        if not new_ttl:
            new_ttl = super(RedisClient, self).ttl(name=name)
            if new_ttl == -2:
                new_ttl = None

        previous = super(RedisClient, self).getset(name=name, value=value)
        if new_ttl and new_ttl > 0:
            super(RedisClient, self).expire(name=name, time=new_ttl)
        return previous

    # TODO: Implement hsetnx with simplified name and ttl.
