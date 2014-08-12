__author__ = 'hunt3r'

"""
A base handlers module
"""
import json
import tornado.web
from bson.objectid import ObjectId
import logging
from pymongo.errors import InvalidDocument, InvalidOperation, InvalidId
from bson import json_util
from jsonschema import ValidationError
from document import AsyncSchedulableDocumentRevisionStack, BaseAsyncMotorDocument
from tornado.gen import coroutine
from tornado.web import authenticated
from tornado.gen import Return
import uuid

GLOBAL_RESERVED_QUERYSTRING_PARAMS = []

class BaseHandler(tornado.web.RequestHandler):
    """A class to collect common handler methods 
    * all other handlers should subclass this across apps.
    """

    def initialize(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_json(self):
        """Load JSON from the request body and store them in
        self.request.arguments, like Tornado does by default for POSTed form
        parameters.

        If JSON cannot be decoded, raises an HTTPError with status 400.
        """
        try:
            self.request.arguments = json.loads(self.request.body)
        except ValueError:
            msg = "Could not decode JSON: %s" % self.request.body
            self.logger.debug(msg)
            self.raise_error(400, msg)

    def get_json_argument(self, name, default=None):
        """
            Find and return the argument with key 'name'
            from JSON request data. Similar to Tornado's get_argument() method.
        """
        if default is None:
            default = self._ARG_DEFAULT
        if not self.request.arguments:
            self.load_json()
        if name not in self.request.arguments:
            if default is self._ARG_DEFAULT:
                msg = "Missing argument '%s'" % name
                self.logger.debug(msg)
                self.raise_error(400, msg)
            self.logger.debug("Returning default argument %s, as we couldn't find "
                    "'%s' in %s" % (default, name, self.request.arguments))
            return default
        arg = self.request.arguments[name]
        return arg

    def get_dict_of_args(self, argList):
        dictionary = {}
        for arg in argList:
            val = self.get_argument(arg, default=None)
            if val and arg != "urbn_key":
                dictionary[arg] = val
        return dictionary

    def get_dict_of_all_args(self):
        """Generates a dictionary from a handler paths query string and returns it

        returns Dictionary
        """
        dictionary = {}
        for arg in [arg for arg in self.request.arguments if arg not in GLOBAL_RESERVED_QUERYSTRING_PARAMS]:
            val =  self.get_argument(arg, default=None)
            if val:
                dictionary[arg] = val
        return dictionary

    def set_arg_value_to_type(self, val):
        """Allow users to pass through truthy type values like true, yes, no and get to a typed variable in your code

        returns

        """
        if val.lower() in ['true', 'yes']:
            return True

        if val.lower() in ['false', 'no']:
            return False

#        if val.isdigit():
#            return int(val)

        return val

    def get_arg_value_as_type(self, key, default=None, convert_int=False):
        val = self.get_query_argument(key, default)

        if isinstance(val, int):
            return val

        if val.lower() in ['true', 'yes']:
            return True

        if val.lower() in ['false', 'no']:
            return False

        return val

    def get_mongo_query_from_arguments(self, reserved_attributes=[]):
        """
        Generate a mongo query from the given URL query parameters,
        handles OR query via multiples
        @return: dictionary
        """

        query = {}
        for arg in self.request.arguments:
            if arg not in reserved_attributes:
                if len(self.request.arguments.get(arg)) > 1:
                    query["$or"] = []
                    for val in self.request.arguments.get(arg):
                        query["$or"].append({arg: self.set_arg_value_to_type(val)})
                else:
                    query[arg] = self.set_arg_value_to_type(self.request.arguments.get(arg)[0])

        return query


    def list_cursor_to_json(self, cursor):
        """Convenience method for converting a mongokit or pymongo list cursor into a JSON object for return"""
        return [self.obj_cursor_to_json(obj) for obj in cursor]


    def obj_cursor_to_json(self, cursor):
        """Handle conversion of pymongo cursor into a JSON object formatted for UI consumption"""
        json_object = json.loads(json_util.dumps(cursor))

        if "_id" in json_object:
            json_object['id'] = str(json_object['_id']['$oid'])
            del json_object['_id']

        # for key in jsonObject:
        #     if isinstance(jsonObject.get(key), dict):
        #         if jsonObject[key].get("$date"):
        #             self.logger.info(jsonObject[key].get("$date"))
        #             jsonObject[key] = jsonObject[key].get("$date")

        return json_object

    def _get_meta_data(self):
        """Creates the meta data dictionary for a revision"""
        return {
            "comment": self.request.headers.get("comment", ""),
            "author": self.get_current_user() or self.settings.get('annonymous_user')
        }

    def json_obj_to_cursor(self, json):
        """Converts a JSON object to a mongo db cursor"""
        cursor = json_util.loads(json)
        if "id" in json:
            cursor["_id"] = ObjectId(cursor["id"])
            del cursor["id"]

        return cursor

    def arg_as_array(self, arg, splitChar="|"):
        """Turns an argument into an array, split by the splitChar"""
        valuesString = self.get_argument(arg, default=None)
        if valuesString:
            valuesArray = valuesString.split(splitChar)
            return valuesArray
        return None

    def raise_error(self, status=500, message="Generic server error.  Out of luck..."):
        """
        Sets an error status and returns a message to the user in JSON format
        @param status:
        @param message:
        """
        self.set_status(status)
        self.write({"message" : message,
                    "status" : status})

    def unauthorized(self):
        """Standard Unauthorized response"""
        self.raise_error(401, "Unauthorized request, please login first")

    def return_resource(self, resource, status=200, statusMessage="OK"):
        self.set_status(status, statusMessage)
        self.write(json.loads(json_util.dumps(resource)))

    def group_objects_by(self, list, attr, valueLabel="value", childrenLabel="children"):
        """
        Generates a group object based on the attribute value on of the given attr value that is passed in.

        @param list: A list of dictionary objects
        @param attr: The attribute that the dictionaries should be sorted upon
        @param valueLabel: What to call the key of the field we're sorting upon
        @param childrenLabel: What to call the list of child objects on the group object
        @return:
        """
        groups = []
        for obj in list:
            val = obj.get(attr)
            if not val:
                pass

            brandName = obj.get("brandName")
            newGroup = {"attribute": attr, valueLabel: val, childrenLabel: [obj]}

            found = False
            for i in range(0,len(groups)):
                if val == groups[i].get(valueLabel):
                    found = True
                    groups[i][childrenLabel].append(obj)
                    pass

            if not found:
                groups.append(newGroup)


        return groups

    def get_current_user(self):
        return self.get_secure_cookie("user")

    def write_hyper_response(self, links=[], meta={}, entity_name=None, entity=None, notifications=[], actions=[]):
        """Writes a hyper media response object"""
        assert entity_name is not None
        assert entity is not None

        meta.update({
            "status": self.get_status()
        })

        self.write({
            "links": links,
            "meta": meta,
            entity_name: entity,
            "notifications": notifications,
            "actions": actions
        })




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

            self.raise_error(404, "%s/%s not found" %(self.object_name, id ))

        except InvalidId, ex:
            self.raise_error(400, message="Your ID is malformed: %s" % id)
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()




    @coroutine
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

                stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, self.settings, master_id=id)
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
                stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, self.settings)

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
            stack = AsyncSchedulableDocumentRevisionStack(collection_name, self.settings, master_id=master_id, )
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
    def put(self, id):
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).put(id)

    @coroutine
    def delete(self, id):
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).delete(id)

    @coroutine
    def post(self, id=None):
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).post(id)


    @coroutine
    def get(self, id):
        """Get a preview of a revision"""
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name for stack")

        self.stack = AsyncSchedulableDocumentRevisionStack(collection_name, self.settings)

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
            stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, self.settings, master_id=id)
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
            self.raise_error(400, "Missing a collection name header")

        self.revisions = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        self.logger.info("Deleting revisions with bulk_id %s" % (bulk_id))

        result = yield self.revisions.collection.remove({ "meta.bulk_id": bulk_id })

        self.write(result)

