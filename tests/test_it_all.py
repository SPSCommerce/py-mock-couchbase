import unittest
from cbmock.connection import MockCouchbaseConnection
import os
from couchbase.exceptions import KeyExistsError, NotFoundError
from babymaker import BabyMaker, StringType, IntType, EnumType, UUIDType
import time
import json



class TestPreloadData(unittest.TestCase):

    def test_preload(self):
        connection = MockCouchbaseConnection(os.path.join(os.path.dirname(__file__), "data"))
        self.assertEquals(connection.data.get("ok"), 'OK')


class TestSetGetDelete(unittest.TestCase):

    def setUp(self):
        self.connection = MockCouchbaseConnection(os.path.join(os.path.dirname(__file__), "data"))

    def test_set_and_get(self):
        data = "There was a little girl with a little curl"
        self.connection.set("test_set_and_get_key", data)
        ghost_data = self.connection.get("test_set_and_get_key")
        self.assertEquals(data, ghost_data.value)

    def test_delete(self):
        data = "laika come home"
        self.connection.set("test_delete", data)
        ghost_data = self.connection.get("test_delete")
        self.assertEquals(data, ghost_data.value)
        self.connection.delete("test_delete")
        with self.assertRaises(NotFoundError):
            self.connection.get("test_delete")
        
    def test_delete_noexist(self):
        with self.assertRaises(NotFoundError):
            self.connection.delete("test_delete_noexist")

class TestGetMulti(unittest.TestCase):

    def setUp(self):
        self.connection = MockCouchbaseConnection(os.path.join(os.path.dirname(__file__), "data"))
        self.user_maker = BabyMaker({
                "id": UUIDType("hex_str"),
                "name": StringType(),
                "age": IntType(min_value=14, max_value=68),
                "gender": EnumType(["Male", "Female"]),
            })
        self.users = list(self.user_maker.make_some(20))
        self.user_ids = [user.get("id") for user in self.users]

    def test_set_and_get(self):
        for user in self.users:
            self.connection.set(user.get("id"), json.dumps(user))
        result = self.connection.get_multi(self.user_ids)
        for user in self.users:
            key = user.get("id")
            stored_user = result.get(key).value
            self.assertIsNotNone(stored_user)
            dict_user = json.loads(stored_user)
            self.assertEquals(user.get("name"), dict_user.get("name"))
            self.assertEquals(user.get("age"), dict_user.get("age"))
            self.assertEquals(user.get("gender"), dict_user.get("gender"))




class TestAdd(unittest.TestCase):

    def setUp(self):
        self.connection = MockCouchbaseConnection()

    def test_add_and_get(self):
        data = "There was a little girl with a little curl"
        self.connection.add("test_add_and_get", data)
        ghost_data = self.connection.get("test_add_and_get")
        self.assertEquals(data, ghost_data.value)

    def test_add_exists(self):
        data = "There was a little girl with a little curl"
        self.connection.add("test_add_exists", data)
        ghost_data = self.connection.get("test_add_exists")
        self.assertIsNotNone(ghost_data)
        with self.assertRaises(KeyExistsError):
            self.connection.add("test_add_exists", data)


class TestReplace(unittest.TestCase):

    def setUp(self):
        self.connection = MockCouchbaseConnection()

    def test_replace_noexist(self):
        data = "There was a little girl with a little curl"
        with self.assertRaises(NotFoundError):
            self.connection.replace("test_replace_noexist", data)

    def test_replace(self):
        data = "There was a little girl with a little curl"
        data_2 = "we believe in nothing"
        key = "test_replace"
        self.connection.add(key, data)
        ghost_data = self.connection.get(key)
        self.assertEquals(data, ghost_data.value)
        self.connection.replace(key, data_2)
        ghost_data = self.connection.get(key)
        self.assertEquals(data_2, ghost_data.value)
        

class TestLocking(unittest.TestCase):

    def setUp(self):
        self.connection = MockCouchbaseConnection()

    def test_lock_and_replace(self):
        data = "Somebody that I used to know"
        data_2 = "Somebody that I used to know, yo"
        key = "test_lock_and_replace"
        self.connection.set(key, data)
        cas = self.connection.lock(key, 2)
        with self.assertRaises(KeyExistsError):
            self.connection.set(key, data)
        self.connection.replace(key, data_2, cas=cas)
        ghost_data = self.connection.get(key)
        self.assertEquals(data_2, ghost_data.value)
        
    def test_lock_and_set(self):
        data = "Somebody that I used to know"
        data_2 = "Somebody that I used to know, yo"
        key = "test_lock_and_replace"
        self.connection.set(key, data)
        cas = self.connection.lock(key, 2)
        with self.assertRaises(KeyExistsError):
            self.connection.set(key, data)
        self.connection.set(key, data_2, cas=cas)
        ghost_data = self.connection.get(key)
        self.assertEquals(data_2, ghost_data.value)
        
    def test_lock_and_unlock_and_set(self):
        data = "Somebody that I used to know"
        data_2 = "Somebody that I used to know, yo"
        key = "test_lock_and_unlock_and_set"
        self.connection.set(key, data)
        cas = self.connection.lock(key, 2)
        with self.assertRaises(KeyExistsError):
            self.connection.set(key, data)
        self.connection.set(key, data_2, cas=cas)
        ghost_data = self.connection.get(key)
        self.assertEquals(data_2, ghost_data.value)
        self.connection.unlock(key, cas)
        self.connection.set(key, data)
        ghost_data = self.connection.get(key)
        self.assertEquals(data, ghost_data.value)

    def test_lock_and_wait_for_expire_and_set(self):
        data = "Somebody that I used to know"
        data_2 = "Somebody that I used to know, yo"
        key = "test_lock_and_wait_for_expire_and_set"
        self.connection.set(key, data)
        self.connection.lock(key, 2)
        with self.assertRaises(KeyExistsError):
            self.connection.set(key, data)
        time.sleep(3)
        self.connection.set(key, data_2)
        ghost_data = self.connection.get(key)
        self.assertEquals(data_2, ghost_data.value)



class TestViews(unittest.TestCase):

    def setUp(self):
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        view_dir = os.path.join(os.path.dirname(__file__), "views")
        self.connection = MockCouchbaseConnection(data_dir, view_dir)
        self.user_maker = BabyMaker({
                "id": UUIDType("hex_str"),
                "name": StringType(),
                "age": IntType(min_value=14, max_value=68),
                "gender": EnumType(["Male", "Female"]),
            })
        self.some_babies = list(self.user_maker.make_some(22))
        for baby in self.some_babies:
            self.connection.set(baby.get("id"), baby)

    def test_view_mappings(self):
        gender_counts = {
            "Male": 0,
            "Female": 0,
        }
        for baby in self.some_babies:
            gender_counts[baby.gender] += 1
        results = self.connection.query("default", "all_doc_ids")
        self.assertEquals(len(results), len(self.some_babies))
        results = self.connection.query("default", "all_doc_ids", key=self.some_babies[0].get("id"), include_docs=True)
        # print ""
        # print "-" * 80
        # print json.dumps(self.connection.views["default"]["all_doc_ids"].map_emissions, indent=4)
        # for item in results:
        #     print item.doc.value
        # print "-" * 80
        self.assertEquals(len(results), 1)
        results = self.connection.query("default", "gender", key="Male")
        self.assertEquals(len(results), gender_counts["Male"])
        results = self.connection.query("default", "gender", key="Female")
        self.assertEquals(len(results), gender_counts["Female"])

    def test_view_replace_item(self):
        gender_counts = {
            "Male": 0,
            "Female": 0,
        }
        for baby in self.some_babies:
            gender_counts[baby.gender] += 1
        results = self.connection.query("default", "gender", key="Male")
        self.assertEquals(len(results), gender_counts["Male"])
        results = self.connection.query("default", "gender", key="Female")
        self.assertEquals(len(results), gender_counts["Female"])
        baby = self.some_babies[3]
        baby.gender = "Male" if baby.gender == "Female" else "Female"
        self.connection.replace(baby.id, baby)
        gender_counts = {
            "Male": 0,
            "Female": 0,
        }
        for baby in self.some_babies:
            gender_counts[baby.gender] += 1
        results = self.connection.query("default", "gender", key="Male")
        self.assertEquals(len(results), gender_counts["Male"])
        results = self.connection.query("default", "gender", key="Female")
        self.assertEquals(len(results), gender_counts["Female"])



    def test_view_delete_item(self):
        gender_counts = {
            "Male": 0,
            "Female": 0,
        }
        for baby in self.some_babies:
            gender_counts[baby.gender] += 1
        results = self.connection.query("default", "gender", key="Male")
        self.assertEquals(len(results), gender_counts["Male"])
        results = self.connection.query("default", "gender", key="Female")
        self.assertEquals(len(results), gender_counts["Female"])
        baby = self.some_babies[3]
        self.some_babies.remove(baby)
        self.connection.delete(baby.id)
        gender_counts = {
            "Male": 0,
            "Female": 0,
        }
        for baby in self.some_babies:
            gender_counts[baby.gender] += 1
        results = self.connection.query("default", "gender", key="Male")
        self.assertEquals(len(results), gender_counts["Male"])
        results = self.connection.query("default", "gender", key="Female")
        self.assertEquals(len(results), gender_counts["Female"])


