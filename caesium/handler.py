"""
A base handlers module
"""
import datetime
import json
import tornado.web
from bson import json_util
import logging
from pymongo.errors import *
from bson.objectid import ObjectId
from settings import GLOBAL_RESERVED_QUERYSTRING_PARAMS

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
            "author": self.get_current_user()
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



class BaseRestHandler(BaseHandler):
    """Restful resources share a common convention.  A standard resource should be able to
        inherit from this class and quickly implement a new Document type"""

    def initialize(self):
        """Initializer for the handler"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.model = None

    def get(self, id):
        """Get resource by bson objectID"""
        try:

            resource = self.model.get_from_id(ObjectId(id))

            if resource:
                self.write(self.obj_cursor_to_json(resource))
                return

            self.raise_error(404, "%s not found" % self.model.__class__.__name__.replace("Callable", ""))
        except InvalidId, ex:
            self.raise_error(status=400, message="Your ID is malformed: %s" % id)
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()

    @tornado.web.authenticated
    def put(self, id):
        """Update by bson ObjectId"""
        try:
            resource = self.model.json_obj_to_document(self.request.body)

            resource["_id"] = ObjectId(id)

            resource.save()

            self.write(self.model.obj_cursor_to_json(resource))

        except ValueError, ex:
            self.raise_error(400, "Invalid JSON Body, check formatting. %s" % ex[0])
        except InvalidId, ex:
            self.raise_error(message="Your ID is malformed: %s" % id)
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()

    @tornado.web.removeslash
    @tornado.web.authenticated
    def post(self, id):
        """Create a new resource"""
        try:
            resource = self.model.json_obj_to_document(self.request.body)

            resource.save()

            self.write(self.model.obj_cursor_to_json(resource))

        except ValueError, ex:
            self.raise_error(400, "Invalid JSON Body, check formatting. %s" % ex[0])
        except InvalidOperation, ex:
            self.raise_error(403, message="The resource you are attempting to insert, already exists")
        except InvalidDocument, ex:
            self.raise_error(400, message="Missing required fields")
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()

    @tornado.web.authenticated
    def delete(self, id):
        """Delete a resource by bson id"""
        try:

            resource = self.model.get_from_id(ObjectId(id))

            if resource:
                resource.delete()
                self.write({"message" : "Deleted object: %s" % id})
                return

            self.raise_error(404, "Resource not found")

        except InvalidId, ex:
            self.logger.error(ex)
            self.raise_error(400, message="Your ID is malformed: %s" % id)
        except Exception, ex:
            self.logger.error(ex)
            self.raise_error()

class BaseListHandler(BaseHandler):
    """Can provide a list of objects based on a provided model"""

    def initialize(self):
        """Initializer for the handler"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.model = None

    def get(self):
        objects = self.model.find()
        json = self.list_cursor_to_json(objects)

        self.write(
            {
                "count": len(objects),
                "result": json
            }
        )
