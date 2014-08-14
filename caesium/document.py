__author__ = 'hunt3r'

from pymongo import GEO2D
import json
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
import json.encoder
import datetime, time
import jsonschema
from json import JSONEncoder
import logging
from tornado.gen import Return, coroutine
import copy

"""
Base document module is a place to put base model object functionality
"""

class AsyncRevisionStackManager(object):


    """Find revisions for any document type and action the revision"""

    def __init__(self, settings):
        """
        Constructor

        :param dict settings: The applications settings, typically it is self.settings in a handler
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.settings = settings
        self.client = settings.get("db")
        assert self.client != None

    @coroutine
    def publish(self):
        """
        Iterate over the scheduler collections and apply any actions found
        """

        try:
             for collection in self.settings.get("scheduler").get("collections"):
                yield self.publish_for_collection(collection)
        except Exception, ex:
            self.logger.error(ex)

    @coroutine
    def set_all_revisions_to_in_process(self, ids):
        """
        Set all revisions found to in process, so that other threads will not pick them up.

        :param list ids:
        """

        predicate = {
            "_id" : {
                "$in" : [ ObjectId(id) for id in ids ]
            }
        }

        set = {"$set": { "inProcess": True }}

        yield self.revisions.collection.update(predicate, set, multi=True)


    @coroutine
    def __get_pending_revisions(self):
        """
        Get all the pending revisions after the current time

        :return: A list of revisions
        :rtype: list

        """
        dttime = time.mktime(datetime.datetime.now().timetuple())
        changes = yield self.revisions.find({
            "toa" : {
                "$lt" : dttime,
            },
            "processed": False,
            "inProcess": None
        })
        if len(changes) > 0:
            yield self.set_all_revisions_to_in_process([change.get("id") for change in changes])

        raise Return(changes)

    @coroutine
    def publish_for_collection(self, collection_name):
        """
        Run the publishing operations for a given collection

        :param str collection_name:
        """
        self.revisions = BaseAsyncMotorDocument("%s_revisions" % collection_name, self.settings)

        changes = yield self.__get_pending_revisions()

        if len(changes) > 0:

            self.logger.info("%s revisions will be actioned" % len(changes))

            for change in changes:
                try:
                    self.logger.info("Applying %s action %s - %s to document: %s/%s" % (change.get("action"), change.get("id"), change.get("meta",{}).get("comment", "No Comment"), change.get("collection"), change.get("master_id")))

                    stack = AsyncSchedulableDocumentRevisionStack(
                        change.get("collection"),
                        self.settings,
                        master_id=change.get("master_id")
                    )

                    revision = yield stack.pop()

                    self.logger.debug(revision)

                except Exception, ex:
                    self.logger.error(ex)

class AsyncSchedulableDocumentRevisionStack(object):
    """This class manages a stack of revisions for a given document in a given collection"""
    SCHEMA = {
        "title":"Schedulable Revision Document",
        "type": "object",
        "required": ["toa", "processed", "collection", "master_id", "action", "patch"],
        "properties" : {
            "toa" : {
                "type": "number",
            },
            "processed": {
                "type": "boolean",
            },
            "collection": {
                "type": "string",
            },
            "master_id": {
                "type": "string",
            },
            "action": {
                "type": "string",
            },
            "patch": {
                "type": ["object", "null"],
            },
            "snapshot": {
                "type": "object"
            },
            "meta": {
                "type": "object"
            }

        }
    }

    DELETE_ACTION = "delete"
    UPDATE_ACTION = "update"
    INSERT_ACTION = "insert"


    def __init__(self, collection_name, settings, collection_schema=None, master_id=None):
        """
        Constructor

        :param str collection: The collection you want revision documents on
        :param dict settings: The application settings
        :param dict collection_schema: This is a JSON Schema dictionary to describe the expected object for that type
        :param str master_id: The Master ID for this set of revisions
        """
        self.master_id=master_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self.settings = settings
        self.client = self.settings.get("db")
        assert self.client != None
        self.revisions = []
        self.collection_name = collection_name
        self.collection = BaseAsyncMotorDocument(collection_name, self.settings, schema=collection_schema)
        self.revisions = BaseAsyncMotorDocument("%s_revisions" % collection_name, self.settings, schema=self.SCHEMA)
        self.previews = BaseAsyncMotorDocument("previews", self.settings)

    @coroutine
    def __update_action(self, revision):
        """Update a master document and revision history document

        :param dict revision: The revision dictionary
        """

        patch = revision.get("patch")
        if patch.get("_id"):
            del patch["_id"]

        update_response = yield self.collection.patch(revision.get("master_id"), self.__make_storeable_patch_patchable(patch))

        if update_response.get("n") == 0:
            raise RevisionNotFoundException()

    @coroutine
    def __insert_action(self, revision):
        """
        Handle the insert action type.

        Creates new document to be created in this collection.
        This allows you to stage a creation of an object

        :param dict revision: The revision dictionary

        """

        revision["patch"]["_id"] = ObjectId(revision.get("master_id"))

        insert_response = yield self.collection.insert(revision.get("patch"))

        if not isinstance(insert_response, str):
            raise DocumentRevisionInsertFailed()

    @coroutine
    def __delete_action(self, revision):
        """
        Handle a delete action to a partiular master id via the revision.

        :param dict revision:
        :return:
        """
        delete_response = yield self.collection.delete(revision.get("master_id"))
        if delete_response.get("n") == 0:
            raise DocumentRevisionDeleteFailed()

    @coroutine
    def pop(self):
        """Pop the top revision off the stack back onto the collection at the given id. This method applies the action.

        Note: This assumes you don't have two revisions scheduled closer than a single scheduling cycle.

        """
        revisions = yield self.list()

        if len(revisions) > 0:
            revision = revisions[0]

            # Update type action
            if revision.get("action") == self.UPDATE_ACTION:
                try:
                    yield self.__update_action(revision)
                except Exception, ex:
                    self.logger.error(ex)

            # Insert type update
            if revision.get("action") == self.INSERT_ACTION:
                try:
                    yield self.__insert_action(revision)
                except Exception, ex:
                    self.logger.error(ex)

            #Get the updated object for attachment to the snapshot
            snapshot_object = yield self.collection.find_one_by_id(revision.get("master_id"))

            #Handle delete action here
            if revision.get("action") == self.DELETE_ACTION:
                try:
                    yield self.__delete_action(revision)
                except Exception, ex:
                    self.logger.error(ex)

                snapshot_object = None

            #Update the revision to be in a post-process state including snapshot
            revision_update_response = yield self.revisions.patch(revision.get("id"),
                {
                    "processed" : True,
                    "snapshot" : snapshot_object,
                    "inProcess": False
                }
            )

            if revision_update_response.get("n") == 0:
                raise RevisionUpdateFailed(msg="revision document update failed")

            revision = yield self.revisions.find_one_by_id(revision.get("id"))

            #TODO: Make this callback method something that can be passed in.  This was used in
            #the original implementation to send back to the client via websocket
            #revision_success.send('revision_success', type="RevisionSuccess", data=revision)

            raise Return(revision)

        raise Return(None)

    def __make_patch_storeable(self, patch):
        """Replace all dots with pipes in key names, mongo doesn't like to store keys with dots.

        :param dict patch: The patch that needs to be made storeable and applied in the future
        """
        new_patch = {}
        for key in patch:
            new_patch[key.replace(".", "|")] = patch[key]

        return new_patch

    def __make_storeable_patch_patchable(self, patch):
        """Replace all pipes with dots, transform back into the a namespace path.
        This is done before the $set query is applied to the document

        :param dict patch: The patch that is to be prepared to be applied
        """

        new_patch = {}
        for key in patch:
            new_patch[key.replace("|", ".")] = patch[key]

        return new_patch

    @coroutine
    def push(self, patch=None, toa=None, meta=None):
        """Push a change on to the revision stack for this ObjectId.  Pushing onto the stack is how you
        get revisions to be staged and scheduled for some future time.

        :param dict patch: None Denotes Delete
        :param int toa: Time of action
        :param dict meta: The meta data for this action
        """

        if not meta:
            meta = {}

        if not toa:
            toa = time.mktime(datetime.datetime.now().timetuple())

        if not isinstance(toa, int):
            toa = int(toa)

        #Documents should be stored in bson formats
        if isinstance(patch, dict):
            patch = self.revisions._dictionary_to_cursor(patch)

        action = None

        if isinstance(patch, type(None)):
            action = self.DELETE_ACTION
        elif self.master_id and isinstance(patch, dict):
            action = self.UPDATE_ACTION
            patch = self.__make_patch_storeable(patch)
            yield self._lazy_migration(meta=copy.deepcopy(meta), toa=toa-1)

        elif not self.master_id and isinstance(patch, dict):
            #Scheduled inserts will not have an object ID and one should be generated
            action = self.INSERT_ACTION
            patch["_id"] = ObjectId()
            self.master_id = patch["_id"].__str__()

        elif not action:
            raise RevisionActionNotValid()

        # We shall never store the _id to a patch dictionary
        if patch and patch.get("_id"):
            del patch["_id"]

        change = {
            "toa": toa,
            "processed": False,
            "collection": self.collection_name,
            "master_id": self.master_id,
            "action": action,
            "patch" : None if action == self.DELETE_ACTION else self.collection._dictionary_to_cursor(patch),
            "meta": meta
        }

        jsonschema.validate(change, self.SCHEMA)

        id = yield self.revisions.insert(change)

        raise Return(id)

    @coroutine
    def list(self, toa=None, show_history=False):
        """Return all revisions for this stack

        :param int toa: The time of action as a UTC timestamp
        :param bool show_history: Whether to show historical revisions
        """
        if not toa:
            toa = time.mktime(datetime.datetime.now().timetuple())

        query = {
            "$query": {
                "master_id": self.master_id,
                "processed": show_history,
                "toa" : {"$lte" : toa}
            },
            "$orderby": {
                "toa": 1
            }
        }

        revisions = yield self.revisions.find(query)

        raise Return(revisions)


    @coroutine
    def _lazy_migration(self, patch=None, meta=None, toa=None):
        """
        Handle when a revision scheduling is turned onto a collection that was previously not scheduleable.
        This method will create the first revision for each object before its every used in the context of scheduling.

        :param dict patch: The patch that should be used
        :param dict meta: Meta data for this action
        :param int toa: The time of action
        :return: A legacy revision for a document that was previously
        :rtype: list
        """
        objects = yield self.revisions.find({"master_id": self.master_id}, limit=1)

        if len(objects) > 0:
            raise Return(objects)

        if not patch:
            patch = yield self.collection.find_one_by_id(self.master_id)

        if not toa:
             toa = long(time.mktime(datetime.datetime.now().timetuple()))

        meta["comment"] = "This document was migrated automatically."

        if isinstance(patch, dict) and patch.get("id"):
            del patch["id"]

        if isinstance(patch, dict) and patch.get("_id"):
            del patch["_id"]

        #Here we separate patch and snapshot, and make sure that the snapshot looks like the master document
        snapshot = copy.deepcopy(patch)
        snapshot["id"] = self.master_id
        snapshot["published"] = self.settings.get("scheduler", {}).get("lazy_migrated_published_by_default", False)

        #If no objects are returned, this is some legacy object that needs a first revision
        #Create it here
        legacy_revision = {
            "toa": toa,
            "processed": True,
            "collection": self.collection_name,
            "master_id": self.master_id,
            "action": self.INSERT_ACTION,
            "patch": self.collection._dictionary_to_cursor(patch),
            "snapshot": snapshot,
            "meta": meta,
        }

        response = yield self.revisions.insert(legacy_revision)
        if isinstance(response, str):
            raise Return([legacy_revision])

        raise Return(None)

    @coroutine
    def __create_preview_object_base(self, dct):
        """
        The starting point for a preview of a future object.
        This is the object which will have future revisions iterated and applied to.

        :param dict dct: The starting object dictionary
        :return: The preview object id
        :rtype: str

        """
        if dct.get("_id"):
            del dct["_id"]

        preview_object_id = yield self.previews.insert(dct)

        raise Return(preview_object_id)

    @coroutine
    def preview(self, revision_id):
        """Get an ephemeral preview of a revision with all revisions applied between it and the current state

        :param str revision_id: The ID of the revision state you want to preview the master id at.
        :return: A snapshot of a future state of the object
        :rtype: dict
        """

        target_revision = yield self.revisions.find_one_by_id(revision_id)

        if isinstance(target_revision.get("snapshot"), dict):
            raise Return(target_revision)

        preview_object = None

        if not isinstance(target_revision, dict):
            raise RevisionNotFound()

        revision_collection_client = BaseAsyncMotorDocument(target_revision.get("collection"), self.settings)

        self.master_id = target_revision.get("master_id")

        action = target_revision.get("action")

        if action == self.DELETE_ACTION:
            raise Return(preview_object)

        if action in [self.INSERT_ACTION, self.UPDATE_ACTION]:

            revisions = yield self.list(toa=target_revision.get("toa"))

            if len(revisions) == 0:
                raise NoRevisionsAvailable()

            first_revision = revisions[0]
            current_document = None


            if first_revision.get("action") == self.UPDATE_ACTION:
                current_document = yield revision_collection_client.find_one_by_id(target_revision.get("master_id"))

            elif first_revision.get("action") == self.INSERT_ACTION:
                # If we are doing an insert, the first revision patch is the current state
                current_document = first_revision.get("patch")

            if not current_document:
                raise RevisionNotFound()

            preview_id = yield self.__create_preview_object_base(current_document)

            for revision in revisions:
                patch = revision.get("patch")

                if patch.get("_id"):
                    del patch["_id"]

                yield self.previews.patch(preview_id, self.__make_storeable_patch_patchable(patch))

            preview_object = yield self.previews.find_one_by_id(preview_id)


            preview_object["id"] = target_revision["id"]
            target_revision["snapshot"] = self.collection._obj_cursor_to_dictionary(preview_object)
            target_revision["snapshot"]["id"] = target_revision["master_id"]

            # Delete the last preview
            yield self.previews.delete(preview_id)

        raise Return(target_revision)


    @coroutine
    def peek(self):
        """Return the top object on the stack for this ID

        :returns: The next revision
        :rtype: dict
        """
        revisions = yield self.list()
        raise Return(revisions[0] if len(revisions) > 0 else None)


class BaseAsyncMotorDocument(object):
    """Concrete abstract class for a mongo collection and document interface

    This class simplifies the use of the motor library, encoding/decoding special types, etc
    """

    def __init__(self, collection_name, settings, schema=None, scheduleable=False):

        """
        Constructor

        :param str collection_name: The name of the collection you want to operate on
        :param dict settings: The application settings
        :param dict schema: A JSON Schema definition for this object type, used for validation
        :param bool scheduleable: Whether or not this document is scheduleable

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.settings = settings
        self.client = self.settings.get("db")

        assert self.client != None
        self.scheduleable = scheduleable
        self.collection_name = collection_name
        self.revisions_collection = self.client["revisions"]
        self.collection = self.client[collection_name]
        self.schema = schema

    @coroutine
    def insert(self, dct, toa=None, comment=""):
        """Create a document

        :param dict dct:
        :param toa toa: Optional time of action, triggers this to be handled as a future insert action for a new document
        :param str comment: A comment
        :rtype str:
        :returns string bson id:
        """
        if self.schema:
            jsonschema.validate(dct, self.schema)

        bson_obj = yield self.collection.insert(dct)

        raise Return(bson_obj.__str__())

    @coroutine
    def upsert(self, _id, dct, attribute="_id"):
        """Update or Insert a new document

        :param str _id: The document id
        :param dict dct: The dictionary to set on the document
        :param str attribute: The attribute to query for to find the object to set this data on
        :returns: JSON Mongo client response including the "n" key to show number of objects effected
        """

        mongo_response = yield self.update(_id, dct, upsert=True, attribute=attribute)

        raise Return(mongo_response)

    @coroutine
    def update(self, predicate_value, dct, upsert=False, attribute="_id"):
        """Update an existing document

        :param predicate_value: The value of the predicate
        :param dict dct: The dictionary to update with
        :param bool upsert: Whether this is an upsert action
        :param str attribute: The attribute to query for to find the object to set this data ond
        :returns: JSON Mongo client response including the "n" key to show number of objects effected
        """
        if self.schema:
            jsonschema.validate(dct, self.schema)

        if attribute=="_id" and not isinstance(predicate_value, ObjectId):
            predicate_value = ObjectId(predicate_value)

        predicate = {attribute: predicate_value}


        dct = self._dictionary_to_cursor(dct)

        mongo_response = yield self.collection.update(predicate, dct, upsert)

        raise Return(self._obj_cursor_to_dictionary(mongo_response))


    @coroutine
    def patch(self, predicate_value, attrs, predicate_attribute="_id"):
        """Update an existing document via a $set query, this will apply only these attributes.

        :param predicate_value: The value of the predicate
        :param dict attrs: The dictionary to apply to this object
        :param str predicate_attribute: The attribute to query for to find the object to set this data ond
        :returns: JSON Mongo client response including the "n" key to show number of objects effected
        t"""

        if predicate_attribute=="_id" and not isinstance(predicate_value, ObjectId):
            predicate_value = ObjectId(predicate_value)

        predicate = {predicate_attribute: predicate_value}

        dct = self._dictionary_to_cursor(attrs)

        if dct.get("_id"):
            del dct["_id"]

        set = { "$set": dct }

        mongo_response = yield self.collection.update(predicate, set, False)

        raise Return(self._obj_cursor_to_dictionary(mongo_response))

    @coroutine
    def delete(self, _id):
        """Delete a document or create a DELETE revision

        :param str _id: The ID of the document to be deleted
        :returns: JSON Mongo client response including the "n" key to show number of objects effected
        """
        mongo_response = yield self.collection.remove({"_id": ObjectId(_id)})

        raise Return(mongo_response)

    @coroutine
    def find_one(self, query):
        """Find one wrapper with conversion to dictionary

        :param dict query: A Mongo query
        """
        mongo_response = yield self.collection.find_one(query)
        raise Return(self._obj_cursor_to_dictionary(mongo_response))

    @coroutine
    def find(self, query, orderby=None, order_by_direction=1, page=0, limit=0):
        """Find a document by any criteria

        :param dict query: The query to perform
        :param str orderby: The attribute to order results by
        :param int order_by_direction: 1 or -1
        :param int page: The page to return
        :param int limit: Number of results per page
        :returns: A list of results
        :rtype: list

        """

        cursor = self.collection.find(query)

        if orderby:
            cursor.sort(orderby, order_by_direction)

        cursor.skip(page*limit).limit(limit)

        results = []
        while (yield cursor.fetch_next):
            results.append(self._obj_cursor_to_dictionary(cursor.next_object()))

        raise Return(results)

    @coroutine
    def find_one_by_id(self, _id):
        """
        Find a single document by id

        :param str _id: BSON string repreentation of the Id
        :return: a signle object
        :rtype: dict

        """
        document = (yield self.collection.find_one({"_id": ObjectId(_id)}))
        raise Return(self._obj_cursor_to_dictionary(document))

    @coroutine
    def create_index(self, index, index_type=GEO2D):
        """Create an index on a given attribute

        :param str index: Attribute to set index on
        :param str index_type: See PyMongo index types for further information, defaults to GEO2D index.
        """
        self.logger.info("Adding %s index to stores on attribute: %s" % (index_type, index))
        yield self.collection.create_index([(index, index_type)])

    @coroutine
    def location_based_search(self, lng, lat, distance, unit="miles", attribute_map=None, page=0, limit=50):
        """Search based on location and other attribute filters

        :param long lng: Longitude parameter
        :param long lat: Latitude parameter
        :param int distance: The radius of the query
        :param str unit: The unit of measure for the query, defaults to miles
        :param dict attribute_map: Additional attributes to apply to the location bases query
        :param int page: The page to return
        :param int limit: Number of results per page
        :returns: List of objects
        :rtype: list
        """

        #Determine what type of radian conversion you want base on a unit of measure
        if unit == "miles":
            distance = float(distance/69)
        else:
            distance = float(distance/111.045)

        #Start with geospatial query
        query = {
            "loc" : {
                "$within": {
                    "$center" : [[lng, lat], distance]}
                }
        }

        #Allow querying additional attributes
        if attribute_map:
            query = dict(query.items() + attribute_map.items())

        results = yield self.find(query, page=page, limit=limit)

        raise Return(self._list_cursor_to_json(results))

    def _dictionary_to_cursor(self, obj):
        """
        Take a raw dictionary representation and adapt it back to a proper mongo document dictionary
        :param dict obj: The object to adapt
        :return: a mongo document with complex types for storage in mongo
        :rtype: dict
        """
        if obj.get("id"):
            obj["_id"] = ObjectId(obj.get("id"))
            del obj["id"]

        if isinstance(obj.get("_id"), str):
            obj["_id"] = ObjectId(obj.get("_id"))

        return obj

    def _obj_cursor_to_dictionary(self, cursor):
        """Handle conversion of pymongo cursor into a JSON object formatted for UI consumption

        :param dict cursor: a mongo document that should be converted to primitive types for the client code
        :returns: a primitive dictionary
        :rtype: dict
        """
        if not cursor:
            return cursor

        cursor = json.loads(json.dumps(cursor, cls=BSONEncoder))

        if cursor.get("_id"):
            cursor["id"] = cursor.get("_id")
            del cursor["_id"]

        return cursor

    def _list_cursor_to_json(self, cursor):
        """Convenience method for converting a mongokit or pymongo list cursor into a JSON object for return"""
        return [self._obj_cursor_to_dictionary(obj) for obj in cursor]

class BSONEncoder(JSONEncoder):
    """BSONEncorder is used to transform certain value types to a more desirable format"""

    def default(self, obj, **kwargs):
        """Handles the adapting of special types from mongo"""
        if isinstance(obj, datetime.datetime):
            return time.mktime(obj.timetuple())

        if isinstance(obj, Timestamp):
            return obj.time

        if isinstance(obj, ObjectId):
            return obj.__str__()

        return JSONEncoder.default(self, obj)


class RevisionNotFoundException(Exception):
    pass

class DocumentRevisionInsertFailed(Exception):
    """Occurs when the revisioned document insert fails"""
    pass

class DocumentRevisionDeleteFailed(Exception):
    """Occurs when the async delete process fails"""
    pass

class RevisionUpdateFailed(Exception):
    """Occurs whena revision update cannot be applied"""
    pass

class RevisionActionNotValid(Exception):
    """Invalid revision type"""
    pass

class RevisionNotFound(Exception):
    """Revision was not found"""
    pass

class NoRevisionsAvailable(Exception):
    """No Revisions Available"""
    pass
