__author__ = 'hunter'

from nose.tools import *
from clock.document import BaseAsyncMotorDocument
import tornado
import tornado.testing
import tornado.gen
from base_tests import BaseAsyncTest
test_attr = unicode("foo")
test_val = unicode("bar")

from clock.document import AsyncRevisionStackManager, AsyncSchedulableDocumentRevisionStack, RevisionActionNotValid
import time
from bson import ObjectId
from tornado.testing import gen_test
from nose.tools import raises
import datetime

class TestBaseAsyncMotorDocument(BaseAsyncTest):
    """ Test the Mongo Client funcitons here"""

    def setUp(self):
        """Setup the test, runs before each test"""

        self.mini_doc = {
            "my doc" : "little bitty doc"
        }

        self.test_fixture = {
            "attr1": "attr1_val",
            "date1": time.mktime(datetime.datetime.now().timetuple()),
            "bool_val": True,
            "list_val": ["My Item", "Item 2", 1],
            "loc": [-75.22, 39.25],
            "sub_document" : self.mini_doc
        }

        self.client = BaseAsyncMotorDocument("test_collection")

        BaseAsyncTest.setUp(self)

        # Setup the state of the test
        self.setup_database()

    @tornado.gen.coroutine
    def setup_database(self):
        yield self.client.collection.drop()

    @tornado.testing.gen_test
    def test_01_insert(self):
        """Test creating a store"""
        resp = yield self.client.insert(self.test_fixture)
        ok_(isinstance(resp, str), "Document was not created")

    @tornado.testing.gen_test
    def test_02_find_one_by_id(self):
        """Test get by id"""
        resp = yield self.client.insert(self.test_fixture)
        obj = yield self.client.find_one_by_id(resp.__str__())
        self.assertIsInstance(obj, dict)
        self.assertEqual(obj.get("id"), resp)

    @tornado.testing.gen_test
    def test_03_update(self):
        """Test that the client updates an existing object"""

        resp = yield self.client.insert(self.test_fixture)
        obj = yield self.client.find_one_by_id(resp)
        obj[test_attr] = test_val
        resp = yield self.client.update(resp, obj)
        ok_(resp.get("updatedExisting"), "Update did not succeed.")

    @tornado.testing.gen_test
    def test_04_find(self):
        """Test that the search end point returns the correct number of items"""
        yield self.client.insert(self.test_fixture)
        yield self.client.insert(self.mini_doc)
        stores = yield self.client.find({})
        ok_(len(stores) == 2)

    @tornado.testing.gen_test
    def test_05_location_based_search(self):
        """Test that you can find an object by location based in miles"""
        yield self.client.create_index("loc")
        resp = yield self.client.insert(self.test_fixture)
        ok_(isinstance(resp, str))
        obj = yield self.client.location_based_search(-75.221, 39.251, 100)
        ok_(obj != None)
        ok_(isinstance(obj, list))

    @tornado.testing.gen_test
    def test_06_delete_object(self):
        """Test that we can DELETE an object"""
        resp = yield self.client.insert(self.test_fixture)
        delete_resp = yield self.client.delete(resp)
        ok_(isinstance(delete_resp, dict))
        ok_(delete_resp.get("n") == 1)

    @tornado.testing.gen_test
    def test_07_patch_object(self):
        """Test that we can DELETE an object"""
        resp = yield self.client.insert(self.test_fixture)
        patch_obj = {
            test_attr: test_val
        }
        patch_response = yield self.client.patch(resp, patch_obj)
        self.assertIsInstance(patch_response, dict)
        self.assertEqual(patch_response.get("n"), 1)

        resp2 = yield self.client.find_one_by_id(resp)
        self.assertEqual(resp2.get(test_attr), test_val)

class TestAsyncRevisionStackAndManagerFunctions(BaseAsyncTest):
    """ Test the Mongo Client funcitons here"""
    mini_doc = {
        "my doc" : "little bitty doc"
    }

    test_fixture = {
        "attr1": "attr1_val",
        "date1": time.mktime(datetime.datetime.now().timetuple()),
        "bool_val": True,
        "list_val": ["My Item", "Item 2", 1],
        "loc": [-75.22, 39.25],
        "sub_document" : mini_doc,
        "patch" :  {
            "foo": "bar"
        }
    }

    three_min_past_now = time.mktime((datetime.datetime.now() - datetime.timedelta(minutes=3)).timetuple())
    two_min_past_now = time.mktime((datetime.datetime.now() - datetime.timedelta(minutes=2)).timetuple())
    one_min_past_now = time.mktime((datetime.datetime.now() - datetime.timedelta(minutes=1)).timetuple())
    one_min_from_now = time.mktime((datetime.datetime.now() + datetime.timedelta(minutes=1)).timetuple())

    three_min_ahead_of_now = time.mktime((datetime.datetime.now() + datetime.timedelta(minutes=3)).timetuple())
    now = time.mktime(datetime.datetime.now().timetuple())

    def setUp(self):
        super(self.__class__, self).setUp()
        self.collection = BaseAsyncMotorDocument("test_fixture")
        self.stack = AsyncSchedulableDocumentRevisionStack("test_fixture")
        self.setup_database()

    @tornado.gen.coroutine
    def setup_database(self):
        self.collection.collection.drop()
        self.stack.revisions.collection.drop()
        #self.stack.previews.collection.drop()

    @tornado.testing.gen_test
    def test_push_on_stack(self):
        """Test that a revision is pushed onto the stack and stored into mongo"""
        id = yield self.stack.push(self.test_fixture, self.three_min_past_now)
        self.assertIsInstance(id, str)
        obj_id = ObjectId(id)
        self.assertIsInstance(obj_id, ObjectId)

    @tornado.testing.gen_test
    def test_patch_is_converted_and_storeable(self):
        """Test that a patch can be set with dot namespace safely and applied asynchronously via pop"""
        master_id = yield self.collection.insert(self.test_fixture)
        patch = {
            "patch.baz" : True
        }
        self.stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)
        yield self.stack.push(patch, self.three_min_past_now)
        yield self.stack.pop()
        obj = yield self.collection.find_one_by_id(master_id)

        self.assertEqual(obj.get("patch").get("foo"), "bar")
        self.assertEqual(obj.get("patch").get("baz"), True)


    @tornado.testing.gen_test
    def test_list_of_revisions(self):
        """Test that we can create a list of revisions from a given document in a collection"""
        master_id = yield self.collection.insert(self.test_fixture)
        self.stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)
        id1 = yield self.stack.push(self.test_fixture, self.three_min_past_now)
        id2 = yield self.stack.push(self.test_fixture, self.three_min_ahead_of_now)

        revisions = yield self.stack.list()
        self.assertEqual(len(revisions), 1,msg="Did not receive the correct number of revisions")
        self.assertEqual(revisions[0].get("id"), id1, msg="The order doesn't appear to be correct")

    @tornado.testing.gen_test
    def test_peek_on_stack(self):
        """Test that we get a single object off the top of the stack"""
        master_id = yield self.collection.insert(self.test_fixture)
        self.stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)
        id1 = yield self.stack.push(self.test_fixture, time.mktime(datetime.datetime.strptime('24052010', "%d%m%Y").timetuple()))
        id2 = yield self.stack.push(self.test_fixture, time.mktime(datetime.datetime.now().timetuple()))
        peeked_obj = yield self.stack.peek()

        self.assertIsNotNone(peeked_obj)
        self.assertIsInstance(peeked_obj, dict)
        self.assertEqual(id1, peeked_obj.get("id"))
        self.assertEqual(peeked_obj.get("action"), "update")

    @gen_test
    def test_pop_off_stack(self):
        """Test that our stack.pop method updates all relevant data correctly"""
        master_id = yield self.collection.insert(self.mini_doc)
        self.stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)
        self.mini_doc[test_attr] = test_val
        id1 = yield self.stack.push(self.mini_doc, self.three_min_past_now)
        id2 = yield self.stack.push(self.mini_doc, self.now)
        obj = yield self.stack.pop()
        self.assertIsInstance(obj, dict)
        self.assertEqual(obj["processed"], True)
        self.assertEqual(obj["snapshot"][test_attr], test_val)

        obj_check = yield self.collection.find_one_by_id(master_id)
        self.assertEqual(test_val, obj_check.get(test_attr))

    @gen_test
    def test_publish_scheduled_pop_off_stack_w_manager(self):
        """Test that we can schedule a revision for 3 min ago and that the revision is applied through the manager"""
        master_id = yield self.collection.insert(self.mini_doc)
        manager = AsyncRevisionStackManager()
        stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)

        self.mini_doc[test_attr] = test_val
        yield stack.push(self.mini_doc, self.three_min_past_now)

        # Run the publish method manually
        yield manager.publish()

        #Make sure the live document contains our test values
        obj_check = yield self.collection.find_one_by_id(master_id)
        self.assertEqual(test_val, obj_check.get(test_attr))

    @gen_test
    def test_publish_with_insert_action(self):
        """Test that we can schedule a new collection object"""
        stack = AsyncSchedulableDocumentRevisionStack("test_fixture")
        meta = {
            "comment" : "foo"
        }
        bson_id = yield stack.push(self.mini_doc, ttl=self.three_min_past_now, meta=meta)
        revisions = yield stack.list()
        self.assertEqual(len(revisions), 1)
        revision = revisions[0]
        self.assertEqual(revision.get("action"), "insert")
        # # Run the publish method manually
        # yield stack.pop()
        #
        # #Make sure the live document contains our test values
        # obj_check = yield self.collection.find_one_by_id(master_id)
        # self.assertIsNone(obj_check, "Object did not delete")


    @gen_test
    def test_publish_with_delete_action(self):
        """Test that we can delete a document on a schedule"""
        master_id = yield self.collection.insert(self.mini_doc)
        manager = AsyncRevisionStackManager()
        stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)
        meta = {
            "comment" : "foo"
        }
        yield stack.push(None, ttl=self.three_min_past_now, meta=meta)

        # Run the publish method manually
        yield stack.pop()

        #Make sure the live document contains our test values
        obj_check = yield self.collection.find_one_by_id(master_id)
        self.assertIsNone(obj_check, "Object did not delete")

    @raises(RevisionActionNotValid)
    @gen_test
    def test_stack_push_can_be_invalid_based_on_object_type(self):
        """Test that if we push the wrong type into a patch attribute, we fail correctly"""
        master_id = yield self.collection.insert(self.mini_doc)
        manager = AsyncRevisionStackManager()
        stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)

        yield stack.push("foo", ttl=self.three_min_past_now)
        yield stack.push(False, ttl=self.three_min_past_now)

    @gen_test
    def test_stack_can_produce_snapshot_of_future_revision_of_update_type(self):
        """Test that the stack can create a future state of a document"""
        master_id = yield self.collection.insert(self.test_fixture)

        stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)

        update = {
            test_attr: test_val
        }

        yield stack.push(update, self.three_min_past_now)

        update["new_persistent"] = True
        yield stack.push(update, self.two_min_past_now)

        update["foo"] = "baz"
        id = yield stack.push(update, self.one_min_from_now)

        response = yield stack.preview(id)
        snapshot = response.get("snapshot")
        self.assertIsInstance(snapshot, dict)

        self.assertEqual(snapshot.get("bool_val"), True)
        self.assertEqual(snapshot.get("new_persistent"), True)
        self.assertEqual(snapshot.get("foo"), "baz")

    @gen_test(timeout=50)
    def test_stack_can_produce_snapshot_of_future_revision_of_insert_type(self):
        """Test that the stack can create a future state of a new yet to be created document"""

        stack = AsyncSchedulableDocumentRevisionStack("test_fixture")

        fixture = self.test_fixture

        yield stack.push(self.test_fixture, self.three_min_past_now)

        fixture["baz"] = "bop"
        fixture["new_persistent"] = True
        yield stack.push(fixture, self.two_min_past_now)

        del fixture["new_persistent"]
        fixture["baz"] = "bit"
        id = yield stack.push(fixture, self.one_min_past_now)


        response = yield stack.preview(id)
        snapshot = response.get("snapshot")

        self.assertIsInstance(snapshot, dict)
        self.assertEqual(snapshot.get("bool_val"), True)
        self.assertEqual(snapshot.get("new_persistent"), True)
        self.assertEqual(snapshot.get("baz"), "bit")

    @gen_test(timeout=50)
    def test_stack_can_migrate_a_legacy_object_automatically(self):
        """Test the stack can migrate a legacy object automatically for the user"""
        client = BaseAsyncMotorDocument("test_fixture")
        revisions = BaseAsyncMotorDocument("test_fixture_revisions")

        fixture = self.test_fixture

        master_id = yield client.insert(fixture)

        stack = AsyncSchedulableDocumentRevisionStack("test_fixture", master_id=master_id)

        fixture["baz"] = "bop"
        yield stack.push(self.test_fixture, self.three_min_past_now, meta={"author": "UnitTest", "comment": "Just a test, BRO."})

        fixture["new_persistent"] = True
        yield stack.push(fixture, self.two_min_past_now)

        del fixture["new_persistent"]
        fixture["baz"] = "bit"
        id = yield stack.push(fixture, self.two_min_past_now)

        #response = yield stack.preview(id)
        list = yield revisions.find({"master_id": master_id})
        self.assertEqual(len(list), 4)
