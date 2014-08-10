__author__ = 'hunt3r'

import logging
from pymongo.errors import *
from bson import json_util
from jsonschema import ValidationError
from apps.base.handler import BaseHandler
from apps.base.document import AsyncSchedulableDocumentRevisionStack, BaseAsyncMotorDocument
from tornado.gen import coroutine
from tornado.web import authenticated
from tornado.websocket import WebSocketHandler
from tornado.gen import Return
import uuid

class BaseRestfulMotorHandler(BaseHandler):

    """Handles the restful endpoints for a mongo document resource, also has some concerns on
    how to handle document revision scheduling."""

    def initialize(self):
        """
        Initializer for the app config handler
        """
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.object_name = "Object"

    @coroutine
    def get(self, id):
        """
        Get an by object by unique identifier

        :id string id: the bson id of an object

        """
        try:
            if self.request.headers.get("Id"):
                object = yield self.client.find_one({self.request.headers.get("Id"): id})
            else:
                object = yield self.client.find_one_by_id(id)

            if object:
                self.write(object)
                return

            self.raise_error(404, "%s not found" % self.object_name )

        except InvalidId, ex:
            self.raise_error(400, message="Your ID is malformed: %s" % id)
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()




    @coroutine
    @authenticated
    def put(self, id):
        """
        Update a store by bson ObjectId

        :id: bson ObjectId:
        :json: <store> post body
        :return: Store
        """
        try:
            #Async update flow
            object = json_util.loads(self.request.body)

            ttl = self.request.headers.get("ttl", None)

            obj_check = yield self.client.find_one_by_id(id)
            if not obj_check:
                self.raise_error(404, "Resource not found: %s" % id)
                self.finish()
                return

            if ttl:

                stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, master_id=id)
                revision_id = yield stack.push(object, int(ttl), meta=self._get_meta_data())

                if isinstance(revision_id, str):
                    self.set_header("Ttl", ttl)

                    #We add the id of the original request, because we don't want to infer this
                    #On the client side, as the state of the client code could change easily
                    #We want this request to return with the originating ID as well.
                    object["id"] = id
                    self.return_resource(object)
                else:
                    self.raise_error(404, "Revision not scheduled for object: %s" % id)

            else:
                if object.get("_id"):
                    del object["_id"]

                response = yield self.client.update(id, object)

                if response.get("updatedExisting"):
                    object = yield self.client.find_one_by_id(id)
                    self.return_resource(object)
                else:
                    self.raise_error(404, "Resource not found: %s" % id)

        except ValidationError, vex:
            self.logger.error("Object validation error", vex)
            self.raise_error(400, "Your %s cannot be updated because it is missing required fields, see docs" % self.object_name)
        except ValueError, ex:
            self.raise_error(400, "Invalid JSON Body, check formatting. %s" % ex[0])
        except InvalidId, ex:
            self.raise_error(message="Your ID is malformed: %s" % id)
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()


    @coroutine
    @authenticated
    def post(self, id=None):
        """
        Create a new object resource

        """
        try:

            base_object = json_util.loads(self.request.body)

            #assert not hasattr(base_object, "_id")

            ttl = self.request.headers.get("ttl", None)

            if ttl:
                # Async create flow
                stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name)

                revision_id = yield stack.push(base_object, ttl=int(ttl), meta=self._get_meta_data())
                resource = yield stack.preview(revision_id)

                if isinstance(revision_id, str):
                    self.set_header("Ttl", ttl)
                    self.return_resource(resource.get("snapshot"))
                else:
                    self.raise_error(404, "Revision not scheduled for object: %s" % id)

            else:

                id = yield self.client.insert(base_object)
                base_object = yield self.client.find_one_by_id(id)

                self.return_resource(base_object)

        except ValidationError, vex:
            self.logger.error("Store validation error", vex)
            self.raise_error(400, "Your %s cannot be updated because it is missing required fields, see docs" % self.object_name)
        except ValueError, ex:
            self.raise_error(400, "Invalid JSON Body, check formatting. %s" % ex[0])
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()

    @coroutine
    @authenticated
    def delete(self, id):
        """
        Delete a store resource by bson id
        """
        try:
            response = yield self.client.delete(id)

            if response.get("n") > 0:
                self.write({"message": "Deleted %s object: %s" % (self.object_name, id) })
                return

            self.raise_error(404, "Resource not found")

        except InvalidId, ex:
            self.raise_error(400, message="Your ID is malformed: %s" % id)
        except:
            self.raise_error()



class BaseRevisionList(BaseRestfulMotorHandler):

    def initialize(self):
        """Initializer for the Search Handler"""

        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None

    @coroutine
    def __lazy_migration(self, master_id):
        collection_name = self.request.headers.get("collection")

        if collection_name:
            stack = AsyncSchedulableDocumentRevisionStack(collection_name, master_id=master_id)
            objects = yield stack._lazy_migration(meta=self._get_meta_data())
            raise Return(objects)

        self.raise_error(500, "This object %s/%s didn't exist as a revision, we tried to create it but we failed... Sorry. Please check this object"% (collection_name, master_id))
        raise Return(None)

    @coroutine
    def get(self, master_id):

        collection_name = self.request.headers.get("collection")
        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        limit=self.get_query_argument("limit", 2)
        add_current_revision = self.get_arg_value_as_type("addCurrent", "false")
        show_history = self.get_arg_value_as_type("showHistory", "false")

        objects_processed = []

        if isinstance(limit, unicode):
            limit = int(limit)

        objects = yield self.client.find({"master_id": master_id,
                                     "processed": False},
                                    orderby="ttl",
                                    order_by_direction=1,
                                    page=0,
                                    limit=20)

        # If this is a document that should have a revision and doesn't we orchestratioin
        # creation of the first one
        if len(objects) == 0:

            new_revision = yield self.__lazy_migration(master_id)
            if not new_revision:
                return

        if show_history:
            objects_processed = yield self.client.find({"master_id": master_id, "processed": True},
                                                  orderby="ttl",
                                                  order_by_direction=-1,
                                                  page=0,
                                                  limit=limit)

        elif add_current_revision:
            objects_processed = yield self.client.find({"master_id": master_id, "processed": True},
                                                  orderby="ttl",
                                                  order_by_direction=-1,
                                                  page=0,
                                                  limit=1)
            
        if len(objects_processed) > 0:
            objects_processed = objects_processed[::-1]
            objects_processed[-1]["current"] = True
            objects = objects_processed + objects

        self.write({
            "count": len(objects),
            "results": objects
        })


class RevisionHandler(BaseRestfulMotorHandler):

    def initialize(self):
        """Initializer for the Search Handler"""
        super(self.__class__, self).initialize()
        self.client = None

    @coroutine
    @authenticated
    def put(self, id):
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error("Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).put(id)

    @coroutine
    @authenticated
    def delete(self, id):
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error("Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).delete(id)

    @coroutine
    @authenticated
    def post(self, id=None):
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error("Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).post(id)


    @coroutine
    @authenticated
    def get(self, id):
        """Get a preview of a revision"""
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error("Missing a collection name for stack")

        self.stack = AsyncSchedulableDocumentRevisionStack(collection_name)

        revision = yield self.stack.preview(id)
        self.write(revision)



class BaseMotorSearch(BaseHandler):
    """Handles searching of the stores endpoint"""

    def initialize(self):
        """Initializer for the Search Handler"""
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)

    @coroutine
    def get(self):

        """
        Standard search end point for a resource of any type, override this get method as necessary
        in any specifc sub class.  This is mostly here as a convenience for basic querying functionality.

        """
        objects = yield self.client.find(self.get_mongo_query_from_arguments())

        self.write({
            "count" : len(objects),
            "results": objects
        })
        self.finish()

class BaseBulkScheduleableUpdateHandler(BaseHandler):
    """Bulk update objects by id and patch"""

    def initialize(self):
        self.client = None

    @coroutine
    def put(self, id=None):
        """Update many objects with a single ttl"""

        ttl = self.request.headers.get("ttl")
        if not ttl:
            self.raise_error(400, "Ttl header is required, none found")
            self.finish(self.request.headers.get("ttl"))

        meta = self._get_meta_data()
        meta["bulk_id"] = uuid.uuid4().get_hex()
        ids = self.get_json_argument("ids")
        patch = self.get_json_argument("patch")

        self.get_json_argument("ids", [])
        for id in ids:
            stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, master_id=id)
            stack.push(patch, ttl=ttl, meta=meta)

        self.write({
            "count": len(ids),
            "result": {
                "ids" : ids,
                "ttl" : ttl,
                "patch": patch
            }
        })
        self.finish()

    @coroutine
    def delete(self, bulk_id):
        """Update many objects with a single ttl"""

        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error("Missing a collection name header")

        self.revisions = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        self.logger.info("Deleting revisions with bulk_id %s" % (bulk_id))

        result = yield self.revisions.collection.remove({ "meta.bulk_id": bulk_id })

        self.write(result)

