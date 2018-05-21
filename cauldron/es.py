import logging, aiohttp, json
from asyncio import coroutine, async
logger = logging.getLogger(__name__)


def es_old_new_mapper(data):
    res = {}
    if not data:
        return data
    if isinstance(data, list):
        res = []
        res = [ es_old_new_mapper(dt) for dt in data ]
        return res
    elif isinstance(data, dict):
        for key,val in data.items():
            if key == "type" and isinstance(val, str) and val == "string":
                res[key] = "text"
            elif key == "index" and isinstance(val, str) and val in["not_analyzed","analyzed"]:
                res[key] = True
                if val == "not_analyzed":
                    res['type'] = "keyword"
            else:
                res[key] = es_old_new_mapper(val)
        return res
    return data

class elasticsearch:
    host = None
    port = None
    match_threshold = 1
    HTTP_TIMEOUT = 15
    HTTP_KEEP_ALIVE_TIMEOUT = 15
    indices = None
    session = None


    class indices:
        host = None
        port = None

        @classmethod
        @coroutine
        def exists(cls, index:str):
            url = "http://{}:{}/{}".format(cls.host, cls.port, index)
            response = yield from aiohttp.request('get', url= url, headers={'Content-Type': 'application/json'})
            res = yield from response.text()
            if response.status == 200 :
                return True
            elif response.status == 404 :
                return False
            else :
                raise Exception("CAULDRON ES ERROR : {}".format((yield  from response.text())))

        @classmethod
        @coroutine
        def create(cls, index:str, body:dict):
            url = "http://{}:{}/{}".format(cls.host, cls.port, index)
            body = es_old_new_mapper(body)
            print("----- checking on url {}".format(body))
            response = yield from aiohttp.request('put', url=url, data = json.dumps(body), headers={'Content-Type': 'application/json'})
            result = yield from response.json()
            print ("======response : {}".format(result))
            return result

        @classmethod
        @coroutine
        def delete(cls, index, ignore=None):
            url = "http://{}:{}/{}".format(cls.host, cls.port, index)
            response = yield from aiohttp.request('delete', url= url, headers={'Content-Type': 'application/json'})
            result = yield from response.json()
            if response.status == 200:
                return result
            elif ignore and response.status in ignore:
                return {"acknowledged": True}
            else:
                return result


    @classmethod
    def connect(cls, host, port):
        cls.host = host
        cls.port = port
        tcp_connector = aiohttp.TCPConnector(conn_timeout = cls.HTTP_TIMEOUT,
                                             keepalive_timeout = cls.HTTP_KEEP_ALIVE_TIMEOUT)
        cls.session = aiohttp.ClientSession(connector = tcp_connector)
        cls.url = 'http://{}:{}'.format(host, port)
        cls.indices.host = host
        cls.indices.port = port
        return cls


    @classmethod
    @coroutine
    def exists(cls, index :str, doc_type:str, id):
        url = None
        if doc_type:
            doc_type = "_doc"
        url = "{}/{}/{}/_count".format(cls.url, index, doc_type)
        query = { "query": {"match": {
                              "_id": str(id)
                            }}}
        response = yield from cls.session.request('post', url= url, data= json.dumps(query), headers={'Content-Type': 'application/json'})
        result = yield from response.json()
        return result['count'] > 0


    @classmethod
    @coroutine
    def index(cls, index:str, doc_type:str, body, id , refresh = False):
        url = None
        if id != None:
            url = "{}/_doc/{}?op_type=create".format(cls.url, str(index))
        else:
            url = "{}/_doc?op_type=create".format(cls.url)
        response = yield from cls.session.request('post', url=url, data=body, headers={'Content-Type': 'application/json'})
        result = yield from response.json()
        if refresh:
            async(cls.session.request('post', url="{}/{}/_refresh".format(cls.url, index) ), headers={'Content-Type': 'application/json'})
        return result


    @classmethod
    @coroutine
    def bulk(cls, index:str, body, doc_type, refresh = False):
        url = "{}/{}".format(cls.url, "_bulk")
        for doc in body:
            if body.get('index', None) != None:
                doc['index']['_index'] = index
                if doc_type:
                    doc['index']['_type'] = doc_type
            else:
                raise Exception("key 'index' not found in document {}".format(doc))
        response = yield from cls.session.request('post', url=url, data=json.dumps(body), headers={'Content-Type': 'application/json'})
        results = yield  from response.json()
        if refresh:
            async(cls.session.request('post', url="{}/{}/_refresh".format(cls.url, index) ), headers={'Content-Type': 'application/json'})
        return results

    @classmethod
    @coroutine
    def get(cls, index, doc_type, id):
        url = None
        if doc_type:
            doc_type = "_doc"
        url = "{}/{}/{}/{}".format(cls.url, index, doc_type, id)
        response = yield from cls.session.request('get', url=url, headers={'Content-Type': 'application/json'})
        result = yield from response.json()
        return result

    @classmethod
    @coroutine
    def mget(cls,  index, doc_type, body):
        url = None
        if doc_type:
            doc_type = "_doc"
        url = "{}/{}/{}/_mget".format(cls.url, index, doc_type)
        response = yield from cls.session.request('post', url=url, data=json.dumps(body), headers={'Content-Type': 'application/json'})
        results = yield from response.json()
        return results

    @classmethod
    @coroutine
    def delete(cls, index, doc_type, id, ignore = None):
        if not doc_type:
            doc_type = "_doc"
        url = "{}/{}/{}/{}".format(cls.url, index, doc_type, id)
        response = yield from aiohttp.request('delete', url=url, headers={'Content-Type': 'application/json'})
        result = yield from response.json()
        if response.status == 200:
            return result
        elif ignore and response.status in ignore:
            return {"acknowledged": True}
        else:
            return result

    @classmethod
    @coroutine
    def search(cls, index, doc_type, id, ignore=None):
        if not doc_type:
            doc_type = "_doc"
        url = "{}/{}/{}/{}".format(cls.url, index, doc_type, id)
        response = yield from aiohttp.request('delete', url=url, headers={'Content-Type': 'application/json'})
        result = yield from response.json()
        if response.status == 200:
            return result
        elif ignore and response.status in ignore:
            return {"acknowledged": True}
        else:
            return result











