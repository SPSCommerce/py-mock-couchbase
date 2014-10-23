import os
from couchbase.exceptions import KeyExistsError, NotFoundError
import threading
from cbmock.views import CBMockView
import json


class MockCouchbaseConnection(object):
    """
    Covers basic document operations and locks.

    TODO - counters.
    """

    def __init__(self, data_dir=None, view_dir=None):
        self.locks = dict()
        self.lock_timeouts = dict()
        self.data = dict()
        self.pre_load_data(data_dir)
        self.cas_counter = 100
        self.design_docs = dict()
        self.views = dict()
        self.load_views(view_dir)

    def pre_load_data(self, data_dir):
        if data_dir:
            for dirname, dirnames, filenames in os.walk(data_dir):
                for name in dirnames:
                    if name.startswith(".") or  name.startswith("_"):
                        dirnames.remove(name)
                for filename in filenames:
                    if filename[0] != "_":
                        doc_id, ext = filename.split(".")
                        if doc_id:
                            with open(os.path.join(dirname, filename), 'r') as fp:
                                self.data[doc_id] = fp.read()


    def load_views(self, view_dir, design_name="default"):
        if view_dir:
            design = dict()
            for dirname, dirnames, filenames in os.walk(view_dir):
                for name in dirnames:
                    if name.startswith(".") or  name.startswith("_"):
                        dirnames.remove(name)
                for filename in filenames:
                    if filename[0] != "_":
                        doc_id, ext = filename.split(".")
                        if doc_id:
                            with open(os.path.join(dirname, filename), 'r') as fp:
                                data = fp.read()
                                design[doc_id] = json.loads(data)
            self.design_create(design_name, {"views": design})


    def set(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        if key in self.locks:
            if cas != self.locks[key]:
                raise KeyExistsError("Key exits")
        self.data[key] = value
        self.update_views(key, value)

    def add(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0):
        if key in self.data:
            raise KeyExistsError("Key exits")
        self.data[key] = value
        self.update_views(key, value)

    def replace(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        if key not in self.data:
            raise NotFoundError("not found")
        self.data[key] = value
        self.update_views(key, value)

    def get(self, key, ttl=0, quiet=None, replica=False, no_format=False):
        if key not in self.data:
            raise NotFoundError("not found")
        return ValueResult(key, self.data[key])

    def get_multi(self, keys, ttl=0, quiet=None, replica=False, no_format=False):
        """
        this breaks the pattern in that errors are not propagated.
        """
        results = MultiResult()
        for key in keys:
            try:
                results[key] = self.get(key)
            except:
                results.all_ok = False
                results[key] = None
        return results

    def delete(self, key, cas=0, quiet=None, persist_to=0, replicate_to=0):
        if key not in self.data:
            raise NotFoundError("not found")
        if key in self.locks:
            if cas != self.locks[key]:
                raise KeyExistsError("Key exits")
        del self.data[key]
        self.update_views(key, None)

    def lock(self, key, ttl=0):
        cas = self.cas_counter
        self.cas_counter += 1
        self.locks[key] = cas
        if ttl:
            def unlock():
                try:
                    self.unlock(key, cas)
                except:
                    pass
            timer = threading.Timer(ttl, unlock)
            timer.start()
        return cas

    def unlock(self, key, cas):
        if key in self.locks:
            if cas != self.locks[key]:
                raise KeyExistsError("Key exits")
            del self.locks[key]

    def design_create(self, name, ddoc, use_devmode=True, syncwait=0):
        views = ddoc.get("views", dict())
        self.design_docs[name] = views
        if name not in self.views:
            self.views[name] = dict()
        view_set = self.views[name]
        new_view_names = set([key for key in views])
        cur_view_names = set([key for key in view_set])
        to_remove = cur_view_names.difference(new_view_names)
        to_add = new_view_names.difference(cur_view_names)
        to_update = cur_view_names.intersection(new_view_names)
        for key in to_remove:
            del view_set[key]
        for key in to_add:
            view_info = views.get(key)
            view = CBMockView(self, view_info.get("map"), view_info.get("reduce"))
            view_set[key] = view
        for key in to_update:
            view_info = views.get(key)
            view = view_set[key]
            view.update(view_info.get("map"), view_info.get("reduce"))

    def design_get(self, name, use_devmode=True):
        return ValueResult(name, self.design_docs.get(name, dict()))

    def design_publish(self, name, syncwait=0):
        pass

    def design_delete(self, name, use_devmode=True, syncwait=0):
        if name in self.design_docs:
            del self.design_docs[name]

    def query(self, design_name, view_name, **kwargs):
        if design_name in self.views:
            view_set = self.views[design_name]
            if view_name in view_set:
                view = view_set.get(view_name)
                return view.query(**kwargs)
            else:
                raise Exception("invalid view name")
        else:
            raise Exception("invalid design name")

    def update_views(self, key, value):
        meta = {"id": key}
        doc = value
        for design in self.views.values():
            for view in design.values():
                view.map_item(doc, meta)



class CBMockResult(object):

    def __init__(self, key):
        self.rc = None
        self.success = True
        self.errstr = None
        self.key = key


class OperationResult(CBMockResult):
    pass


class ValueResult(CBMockResult):
    
    def __init__(self, key, value):
        super(ValueResult, self).__init__(key)
        self.value = value


class MultiResult(dict):

    def __init__(self):
        self.all_ok = True




