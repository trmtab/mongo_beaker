# -*- coding: utf-8 -*-

import datetime
import logging
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter

from beaker.container import NamespaceManager, Container
from beaker.synchronization import file_synchronizer
from beaker.util import verify_directory

try:
    import pymongo
    import pymongo.uri_parser
    from pymongo import MongoClient
except ImportError:
    raise InvalidCacheBackendError("MongoDB cache backend requires the pymongo library")

try:
    import cPickle as pickle
except:
    import pickle

log = logging.getLogger(__name__)

class MongoNamespaceManager(NamespaceManager):
    """
    MongoDB backend for beaker

    Configuration example:
        beaker.session.type = mongo
        beaker.session.uri = mongodb://localhost:27017/db_name.collection
    """
    def __init__(self, namespace, uri=None, data_dir=None, lock_dir=None, **params):
        NamespaceManager.__init__(self, namespace)

        if not uri:
            raise MissingCacheParameter("URI is required")

        self.db_connection_params = pymongo.uri_parser.parse_uri(uri)
        if not self.db_connection_params["collection"]:
            raise MissingCacheParameter("invalid URI: missing collection")
        elif not self.db_connection_params["database"]:
            raise MissingCacheParameter("invalid URI: missing database")

        if lock_dir:
            self.lock_dir = lock_dir
        elif data_dir:
            self.lock_dir = data_dir + "/container_mongodb_lock"
        if hasattr(self, "lock_dir"):
            verify_directory(self.lock_dir)

        self.open_connection(uri)

    def open_connection(self, uri):
        self.db = MongoClient(uri)[self.db_connection_params["database"]]
        self.db_collection = self.db_connection_params["collection"]

    def get_creation_lock(self, key):
        return file_synchronizer(identifier="mongodb_container/funclock/%s" % self.namespace,
            lock_dir=self.lock_dir)

    def _format_key(self, key):
        return self.namespace + "_" + key

    def __getitem__(self, key):
        log.debug("Getting %s" % key)
        return pickle.loads(str(self.db[self.db_collection].find_one({ "_id": self._format_key(key) },
            fields=["data"])["data"]))

    def __contains__(self, key):
        log.debug("Contains %s" % key)
        return self.db[self.db_collection].find({ "_id": self._format_key(key) }).count() > 0

    def has_key(self, key):
        return key in self

    def set_value(self, key, value):
        log.debug("Setting %s: %s" % (key, str(value)))
        self.db[self.db_collection].update({ "_id": self._format_key(key) },
            { "$set": { "data": pickle.dumps(value), "timestamp": datetime.datetime.utcnow() } }, upsert=True)

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def __delitem__(self, key):
        log.debug("Deleting %s" % key)
        self.db[self.db_collection].remove({ "_id": self._format_key(key) })

    def do_remove(self):
        log.debug("Removing %s" % self.db_collection)
        self.db.drop_collection(self.db_collection)

    def keys(self):
        log.debug("Retrieving keys from  %s" % self.db_collection)
        return [item["_id"].replace(self.namespace + "_", "") for item in self.db[self.db_collection].find()]

class MongoManagerContainer(Container):
    namespace_class = MongoNamespaceManager

