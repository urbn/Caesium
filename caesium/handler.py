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

class BaseHandler(tornado.web.RequestHandler):
    """A class to collect common handler methods that can be useful in your individual implementation,
    this includes functions for working with query strings and Motor/Mongo type documents
    """

    def initialize(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_json(self):
        """Load JSON from the request body and store them in
        self.request.arguments, like Tornado does by default for POSTed form
        parameters.

        If JSON cannot be decoded

        :raises ValueError: JSON Could not be decoded
        """
        try:
            self.request.arguments = json.loads(self.request.body)
        except ValueError:
            msg = "Could not decode JSON: %s" % self.request.body
            self.logger.debug(msg)
            self.raise_error(400, msg)

    def get_json_argument(self, name, default=None):
        """Find and return the argument with key 'name'
        from JSON request data. Similar to Tornado's get_argument() method.

        :param str name: The name of the json key you want to get the value for
        :param bool default: The default value if nothing is found
        :returns: value of the argument name request
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

    def get_dict_of_all_args(self):
        """Generates a dictionary from a handler paths query string and returns it

        :returns: Dictionary of all key/values in arguments list
        :rtype: dict
        """
        dictionary = {}
        for arg in [arg for arg in self.request.arguments if arg not in self.settings.get("reserved_query_string_params", [])]:
            val =  self.get_argument(arg, default=None)
            if val:
                dictionary[arg] = val
        return dictionary

    def get_arg_value_as_type(self, key, default=None, convert_int=False):
        """Allow users to pass through truthy type values like true, yes, no and get to a typed variable in your code

        :param str val: The string reprensentation of the value you want to convert
        :returns: adapted value
        :rtype: dynamic
        """

        val = self.get_query_argument(key, default)

        if isinstance(val, int):
            return val

        if val.lower() in ['true', 'yes']:
            return True

        if val.lower() in ['false', 'no']:
            return False

        return val

    def get_mongo_query_from_arguments(self, reserved_attributes=[]):
        """Generate a mongo query from the given URL query parameters, handles OR query via multiples

        :param list reserved_attributes: A list of attributes you want to exclude from this particular query
        :return: dict
        """

        query = {}
        for arg in self.request.arguments:
            if arg not in reserved_attributes:
                if len(self.request.arguments.get(arg)) > 1:
                    query["$or"] = []
                    for val in self.request.arguments.get(arg):
                        query["$or"].append({arg: self.get_arg_value_as_type(val)})
                else:
                    query[arg] = self.get_arg_value_as_type(self.request.arguments.get(arg)[0])

        return query

    def list_cursor_to_json(self, cursor):
        """Convenience method for converting a mongokit or pymongo list cursor into a JSON object for return
        :param Cursor cursor: A motor client database cursor
        """
        return [self.obj_cursor_to_json(obj) for obj in cursor]


    def obj_cursor_to_json(self, cursor):
        """Handle conversion of pymongo cursor into a JSON object formatted for UI consumption

        :param Cursor cursor: A motor client database cursor
        """
        json_object = json.loads(json_util.dumps(cursor))

        if "_id" in json_object:
            json_object['id'] = str(json_object['_id']['$oid'])
            del json_object['_id']

        return json_object

    def _get_meta_data(self):
        """Creates the meta data dictionary for a revision"""
        return {
            "comment": self.request.headers.get("comment", ""),
            "author": self.get_current_user() or self.settings.get('annonymous_user')
        }

    def json_obj_to_cursor(self, json):
        """(Deprecated) Converts a JSON object to a mongo db cursor

        :param str json: A json string
        :returns: dictionary with ObjectId type
        :rtype: dict
        """
        cursor = json_util.loads(json)
        if "id" in json:
            cursor["_id"] = ObjectId(cursor["id"])
            del cursor["id"]

        return cursor

    def arg_as_array(self, arg, split_char="|"):
        """Turns an argument into an array, split by the splitChar

        :param str arg: The name of the query param you want to turn into an array based on the value
        :param str split_char: The character the value should be split on.
        :returns: A list of values
        :rtype: list
        """
        valuesString = self.get_argument(arg, default=None)
        if valuesString:
            valuesArray = valuesString.split(split_char)
            return valuesArray

        return None

    def raise_error(self, status=500, message="Generic server error.  Out of luck..."):
        """
        Sets an error status and returns a message to the user in JSON format

        :param int status: The status code to use
        :param str message: The message to return in the JSON response
        """
        self.set_status(status)
        self.write({"message" : message,
                    "status" : status})

    def unauthorized(self, message="Unauthorized request, please login first"):
        """Standard Unauthorized response

        :param str message: The Message to use in the error response
        """
        self.raise_error(401, message)

    def return_resource(self, resource, status=200, statusMessage="OK"):
        """Return a resource response

        :param str resource: The JSON String representation of a resource response
        :param int status: Status code to use
        :param str statusMessage: The message to use in the error response
        """
        self.set_status(status, statusMessage)
        self.write(json.loads(json_util.dumps(resource)))


    def group_objects_by(self, list, attr, valueLabel="value", childrenLabel="children"):
        """
        Generates a group object based on the attribute value on of the given attr value that is passed in.

        :param list list: A list of dictionary objects
        :param str attr: The attribute that the dictionaries should be sorted upon
        :param str valueLabel: What to call the key of the field we're sorting upon
        :param str childrenLabel: What to call the list of child objects on the group object
        :returns: list of grouped objects by a given attribute
        :rtype: list
        """

        groups = []
        for obj in list:
            val = obj.get(attr)
            if not val:
                pass

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
        """Gets the current user from the secure cookie store

        :returns: user name for logged in user
        :rtype: str
        """
        return self.get_secure_cookie(self.settings.get("session_cookie", "user"))

    def write_hyper_response(self, links=[], meta={}, entity_name=None, entity=None, notifications=[], actions=[]):
        """Writes a hyper media response object

        :param list links: A list of links to the resources
        :param dict meta: The meta data for this response
        :param str entity_name: The entity name
        :param object entity: The Entity itself
        :param list notifications: List of notifications
        :param list actions: List of actions
        """
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
        Initialize the base handler
        """
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.object_name = "Object"

    @coroutine
    def get(self, id):
        """
        Get an by object by unique identifier

        :id string id: the bson id of an object
        :rtype: JSON
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
        Update a resource by bson ObjectId

        :returns: json string representation
        :rtype: JSON
        """
        try:
            #Async update flow
            object = json_util.loads(self.request.body)

            toa = self.request.headers.get("Caesium-TOA", None)

            obj_check = yield self.client.find_one_by_id(id)
            if not obj_check:
                self.raise_error(404, "Resource not found: %s" % id)
                self.finish()
                return

            if toa:

                stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, self.settings, master_id=id)
                revision_id = yield stack.push(object, int(toa), meta=self._get_meta_data())

                if isinstance(revision_id, str):
                    self.set_header("Caesium-TOA", toa)

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
            self.logger.error("%s validation error" % self.object_name, vex)
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

        :json: Object to create
        :returns: json string representation
        :rtype: JSON

        """
        try:

            base_object = json_util.loads(self.request.body)

            #assert not hasattr(base_object, "_id")

            toa = self.request.headers.get("Caesium-TOA", None)

            if toa:
                # Async create flow
                stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, self.settings)

                revision_id = yield stack.push(base_object, toa=int(toa), meta=self._get_meta_data())
                resource = yield stack.preview(revision_id)

                if isinstance(revision_id, str):
                    self.set_header("Caesium-TOA", toa)
                    self.return_resource(resource.get("snapshot"))
                else:
                    self.raise_error(404, "Revision not scheduled for object: %s" % id)

            else:

                id = yield self.client.insert(base_object)
                base_object = yield self.client.find_one_by_id(id)

                self.return_resource(base_object)

        except ValidationError, vex:
            self.logger.error("%s validation error" % self.object_name, vex)
            self.raise_error(400, "Your %s cannot be created because it is missing required fields, see docs" % self.object_name)
        except ValueError, ex:
            self.raise_error(400, "Invalid JSON Body, check formatting. %s" % ex[0])
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()

    @coroutine
    def delete(self, id):
        """
        Delete a resource by bson id
        :raises: 404 Not Found
        :raises: 400 Bad request
        :raises: 500 Server Error
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

        self.finish()

class BaseRevisionList(BaseRestfulMotorHandler):

    def initialize(self):
        """Initializer for the Search Handler"""

        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None

    @coroutine
    def __lazy_migration(self, master_id):
        """
        Creates a revision for a master id that didn't previously have a revision, this allows
        you to easily turn on revisioning for a collection that didn't previously allow for it.

        :param master_id:
        :returns: list of objects
        """
        collection_name = self.request.headers.get("collection")

        if collection_name:
            stack = AsyncSchedulableDocumentRevisionStack(collection_name, self.settings, master_id=master_id, )
            objects = yield stack._lazy_migration(meta=self._get_meta_data())
            raise Return(objects)

        self.raise_error(500, "This object %s/%s didn't exist as a revision, we tried to create it but we failed... Sorry. Please check this object"% (collection_name, master_id))
        raise Return(None)

    @coroutine
    def get(self, master_id):
        """
        Get a list of revisions by master ID

        :param master_id:
        :return:
        """
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
                                    orderby="toa",
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
                                                  orderby="toa",
                                                  order_by_direction=-1,
                                                  page=0,
                                                  limit=limit)

        elif add_current_revision:
            objects_processed = yield self.client.find({"master_id": master_id, "processed": True},
                                                  orderby="toa",
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
        """
        Update a revision by ID

        :param id: BSON id
        :return:
        """

        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).put(id)

    @coroutine
    def delete(self, id):
        """
        Delete a revision by ID

        :param id: BSON id
        :return:
        """

        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).delete(id)

    @coroutine
    def post(self, id=None):
        """
        Create a revision manually without the stack

        :param id: BSON id
        :return: JSON
        """
        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.client = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        super(self.__class__, self).post(id)


    @coroutine
    def get(self, id):
        """
        Get revision based on the stack preview algorithm

        :param id: BSON id
        :return: JSON
        """
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
        in any specifc sub class.  This is mostly here as a convenience for basic querying functionality
        on attribute

        example URL::

            foo?attr1=foo&attr2=true

        will create a query of::

            {
                "attr1": "foo",
                "attr2": true
            }


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
        """Update many objects with a single PUT.

        Example Request::

            {
                "ids": ["52b0ede98ac752b358b1bd69", "52b0ede98ac752b358b1bd70"],
                "patch": {
                    "foo": "bar"
                }
            }

        """

        toa = self.request.headers.get("Caesium-TOA")

        if not toa:
            self.raise_error(400, "Caesium-TOA header is required, none found")
            self.finish(self.request.headers.get("Caesium-TOA"))

        meta = self._get_meta_data()
        meta["bulk_id"] = uuid.uuid4().get_hex()
        ids = self.get_json_argument("ids")
        patch = self.get_json_argument("patch")

        self.get_json_argument("ids", [])
        for id in ids:
            stack = AsyncSchedulableDocumentRevisionStack(self.client.collection_name, self.settings, master_id=id)
            stack.push(patch, toa=toa, meta=meta)

        self.write({
            "count": len(ids),
            "result": {
                "ids" : ids,
                "toa" : toa,
                "patch": patch
            }
        })
        self.finish()

    @coroutine
    def delete(self, bulk_id):
        """Update many objects with a single toa

        :param str bulk_id: The bulk id for the job you want to delete
        """

        collection_name = self.request.headers.get("collection")

        if not collection_name:
            self.raise_error(400, "Missing a collection name header")

        self.revisions = BaseAsyncMotorDocument("%s_revisions" % collection_name)

        self.logger.info("Deleting revisions with bulk_id %s" % (bulk_id))

        result = yield self.revisions.collection.remove({ "meta.bulk_id": bulk_id })

        self.write(result)

