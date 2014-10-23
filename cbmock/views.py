import subprocess
import json
from traceback import print_exc



emit_func = '''
        function emit(key, value) {
            console.log(JSON.stringify({key: key, value: value}));
        }
    '''


class CBMockView(object):
    """
        TODO - reduce
        TODO - make PyV8 work, shelling out to node is slow.
    """
    def __init__(self, connection, map_func, reduce_func=None):
        self.connection = connection
        self.map_func = map_func
        self.reduce_func = reduce_func
        self._process(["node", "--version"])
        self.map_emissions = dict()

    def _process(self, cmd, input_data=None):
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        stdout, stderr = process.communicate(input=input_data)
        if stderr:
            raise Exception(stderr)
        return stdout

    def _process_all(self):
        self.map_emissions = dict()
        items = self.connection.data
        for key, value in items.iteritems():
            self.map_item(value, {"id": key})
        self._reduce_all()


    # def _reduce_all(self):
    #     pass

    def update(self, map_func, reduce_func=None):
        # reprocess = False
        # reduce_changed = False
        if map_func != self.map_func:
            self.map_func = map_func
            self._process_all()
            # reprocess = True
        # if reduce_func != self.reduce_func:
        #     self.reduce_func = reduce_func
        #     reduce_changed = True
        # if reprocess:
        #     self._process_all()
        # elif reduce_changed:
        #     self._reduce_all()


    def map_item(self, document, meta_data):
        doc = document if isinstance(document, basestring) else json.dumps(document)
        meta = json.dumps(meta_data)
        # go through and remove this object from all emissions if it exists already
        for emissions in self.map_emissions.values():
            for emission in emissions:
                _meta = emission.get("meta")
                if _meta.get("id") == meta_data["id"]:
                    emissions.remove(emission)
        if document:
            # if document is None then all we needed to do was remove it from the view emissions
            try:
                data = self._process(["node"], "{3};var map = {0};map({1}, {2});".format(self.map_func, doc, meta, emit_func))
                if data:
                    obj = json.loads(data)
                    key = obj.get("key")
                    value = obj.get("value")
                    if key not in self.map_emissions:
                        self.map_emissions[key] = list()
                    self.map_emissions[key].append({"meta": meta_data, "value": value})
            except:
                print_exc()

    def delete_from_view(self, document, meta_data):
        pass

    def query(self, key=None, reduce=False, include_docs=False, query=None, **kwargs):
        # TODO - support multi, range, and reduce
        results = list()
        if key:
            data = self.map_emissions.get(key)
            for item in data:
                meta = item.get("meta")
                doc = None
                if include_docs:
                    doc = self.connection.get(meta.get("id"))
                results.append(CBMockViewRow(key, item.get("value"), meta.get("id"), doc))
        elif query:
            start = 0
            end = CBMockQuery.STRING_RANGE_END
            if query.mapkey_range:
                start = query.mapkey_range[0]
                end = query.mapkey_range[0]
            elif query.startkey or query.endkey:
                start = query.startkey or 0
                end = query.endkey or CBMockQuery.STRING_RANGE_END
            for emissions_key in self.map_emissions:
                if emissions_key >= start and emissions_key <= end:
                    data = self.map_emissions.get(emissions_key)
                    for item in data:
                        meta = item.get("meta")
                        doc = None
                        if include_docs:
                            doc = self.connection.get(meta.get("id"))
                        results.append(CBMockViewRow(key, item.get("value"), meta.get("id"), doc))
        else:
            for emissions_key in self.map_emissions:
                data = self.map_emissions.get(emissions_key)
                for item in data:
                    meta = item.get("meta")
                    doc = None
                    if include_docs:
                        doc = self.connection.get(meta.get("id"))
                    results.append(CBMockViewRow(emissions_key, item.get("value"), meta.get("id"), doc))
        return results


class CBMockViewRow(object):

    def __init__(self, key, value, docid, doc=None):
        self.key = key
        self.value = value
        self.docid = docid
        self.doc = doc


class CBMockQuery(object):
    
    STRING_RANGE_END = json.loads('"\u0FFF"')

    def __init__(self, startkey=None, endkey=None, mapkey_range=None):
        self.startkey = startkey
        self.endkey = endkey
        self.mapkey_range = mapkey_range



