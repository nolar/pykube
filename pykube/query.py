import json
from collections import namedtuple
from urllib.parse import urlencode

import aiohttp

from ._syncasync import AsyncSyncMixin, SyncWrapper
from .exceptions import ObjectDoesNotExist
from .http import AsyncHTTPClient, SyncedHTTPClient

all_ = object()
everything = object()
now = object()


class Table:
    """
    Tabular resource representation
    See https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables
    """
    def __init__(self, api_obj_class, obj: dict):
        assert obj['kind'] == 'Table'
        self.api_obj_class = api_obj_class
        self.obj = obj

    def __repr__(self):
        return "<Table of {kind} at {address}>".format(kind=self.api_obj_class.kind, address=hex(id(self)))

    @property
    def columns(self):
        return self.obj['columnDefinitions']

    @property
    def rows(self):
        return self.obj['rows']


class AsyncQueryImpl:

    def __init__(self, api, api_obj_class, namespace=None):
        super().__init__()
        self.api = api
        self.api_obj_class = api_obj_class
        self.namespace = namespace
        self.selector = everything
        self.field_selector = everything

    def __repr__(self):
        return "<Query of {kind} at {address}>".format(kind=self.api_obj_class.kind, address=hex(id(self)))

    def all(self):
        return self._clone()

    def filter(self, namespace=None, selector=None, field_selector=None):
        '''
        Filter objects by namespace, labels, or fields

        :param namespace: Namespace to filter by (pass pykube.all to get objects in all namespaces)
        :param selector: Label selector, can be a dictionary of label names/values
        '''
        clone = self._clone()
        if namespace is not None:
            clone.namespace = namespace
        if selector is not None:
            clone.selector = selector
        if field_selector is not None:
            clone.field_selector = field_selector
        return clone

    def _clone(self, cls=None):
        if cls is None:
            cls = self.__class__
        clone = cls(self.api, self.api_obj_class, namespace=self.namespace)
        clone.selector = self.selector
        clone.field_selector = self.field_selector
        return clone

    def _build_api_url(self, params=None):
        if params is None:
            params = {}
        if self.selector is not everything:
            params["labelSelector"] = as_selector(self.selector)
        if self.field_selector is not everything:
            params["fieldSelector"] = as_selector(self.field_selector)
        query_string = urlencode(params)
        return "{}{}".format(self.api_obj_class.endpoint, "?{}".format(query_string) if query_string else "")

    @property
    def api(self):
        """
        Get an async client for async activities.

        In most cases, async methods are wrapped and put as the sync ones.
        If such clients are used in the async methods, the event loop will
        be blocked. Instead, the wrapped methods should use an async client
        internally even if ``self`` is a sync object and a client is sync.

        The sync client cannot be unwrapped on the creation, and should remain
        sync -- in case some of real sync methods need the real sync client.

        If a normal async client is passed (i.e. no wrapping is used),
        it is used as is.
        """
        return self._api

    @api.setter
    def api(self, value):
        self._api = value
        if isinstance(value, AsyncHTTPClient):
            self.sync_api = SyncedHTTPClient(value)
            self.async_api = value
        elif isinstance(value, SyncedHTTPClient):
            self.sync_api = value
            self.async_api = value.async_wrapped
        else:
            raise RuntimeError(f"API client type is not supported: {value!r}")

    async def get_by_name(self, name):
        '''
        Get object by name, raises ObjectDoesNotExist if not found
        '''
        kwargs = {
            "url": "{}/{}".format(self.api_obj_class.endpoint, name),
            "namespace": self.namespace,
        }
        if self.api_obj_class.base:
            kwargs["base"] = self.api_obj_class.base
        if self.api_obj_class.version:
            kwargs["version"] = self.api_obj_class.version
        r = await self.async_api.get(**kwargs)
        try:
            r.raise_for_status()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                raise ObjectDoesNotExist("{} does not exist.".format(name))
            else:
                raise
        # if not r.ok:
        #     if r.status_code == 404:
        #         raise ObjectDoesNotExist("{} does not exist.".format(name))
        #     self.api.raise_for_status(r)
        data = await r.json()
        return self.api_obj_class(self.api, data)

    async def get(self, *args, **kwargs):
        '''
        Get a single object by name, namespace, label, ..
        '''
        if "name" in kwargs:
            return await self.get_by_name(kwargs["name"])
        clone = self.filter(*args, **kwargs)
        num = len(clone)
        if num == 1:
            return clone.query_cache["objects"][0]
        if not num:
            raise ObjectDoesNotExist("get() returned zero objects")
        raise ValueError("get() more than one object; use filter")

    async def get_or_none(self, *args, **kwargs):
        '''
        Get object by name, return None if not found
        '''
        try:
            return await self.get(*args, **kwargs)
        except ObjectDoesNotExist:
            return None

    def watch(self, since=None, *, params=None):
        query = self._clone(WatchQuery)
        query.params = params
        if since is now:
            query.resource_version = self.response["metadata"]["resourceVersion"]
        elif since is not None:
            query.resource_version = since
        return query

    async def execute(self, **kwargs):
        kwargs["url"] = self._build_api_url()
        if self.api_obj_class.base:
            kwargs["base"] = self.api_obj_class.base
        if self.api_obj_class.version:
            kwargs["version"] = self.api_obj_class.version
        if self.namespace is not None and self.namespace is not all_:
            kwargs["namespace"] = self.namespace
        r = await self.async_api.get(**kwargs)
        r.raise_for_status()
        return r

    async def as_table(self) -> Table:
        """
        Execute query and return result as Table (similar to what kubectl does)
        See https://kubernetes.io/docs/reference/using-api/api-concepts/#receiving-resources-as-tables
        """
        response = await self.execute(headers={'Accept': 'application/json;as=Table;v=v1beta1;g=meta.k8s.io'})
        return Table(self.api_obj_class, await response.json())

    async def iterator(self):
        """
        Execute the API request and return an iterator over the objects. This
        method does not use the query cache.
        """
        for obj in (self.execute().json().get("items") or []):
            yield self.api_obj_class(self.api, obj)

    async def get_query_cache(self):
        if not hasattr(self, "_query_cache"):
            cache = {"objects": []}
            response = await self.execute()
            result = await response.json()
            cache["response"] = result
            for obj in (cache["response"].get("items") or []):
                cache["objects"].append(self.api_obj_class(self.api, obj))
            self._query_cache = cache
        return self._query_cache

    @property  # return coro; specially handled in syncasync converter.
    async def response(self):
        cache = await self.get_query_cache()
        return cache["response"]

    def __len__(self):
        # FIXME: same sync problem as .response
        return len(self.get_query_cache()["objects"])

    def __aiter__(self):
        # TODO: make it an async iter actually.
        return iter(self.get_query_cache()["objects"])


class AsyncWatchQueryImpl(AsyncQueryImpl):

    def __init__(self, *args, **kwargs):
        self.resource_version = kwargs.pop("resource_version", None)
        self.params = None
        super().__init__(*args, **kwargs)
        self._response = None

    @property
    def response(self):
        return self._response

    async def object_stream(self):
        params = dict(self.params or {})  # shallow clone for local use
        params["watch"] = "true"
        if self.resource_version is not None:
            params["resourceVersion"] = self.resource_version
        kwargs = {
            "url": self._build_api_url(params=params),
            # "stream": True,
        }
        if self.namespace is not all_:
            kwargs["namespace"] = self.namespace
        if self.api_obj_class.version:
            kwargs["version"] = self.api_obj_class.version
        r = await self.async_api.get(**kwargs)
        await self.async_api.raise_for_status(r)
        self._response = r
        WatchEvent = namedtuple("WatchEvent", "type object")
        async with r:
            async for line in r.content:
                we = json.loads(line.decode("utf-8"))
                yield WatchEvent(type=we["type"], object=self.api_obj_class(self.api, we["object"]))

    def __aiter__(self):
        return iter(self.object_stream())


class SyncedQueryImpl(SyncWrapper, async_cls=AsyncQueryImpl):
    pass


class SyncedWatchQueryImpl(SyncWrapper, async_cls=AsyncWatchQueryImpl):
    # TODO: SyncWatchQuery must have sync iterators! => def object_stream():
    def __iter__(self):
        itr = self.object_stream()
        return iter(itr)


# Backward compatibility: the class name as it was used in pre-async time.
# Auto-decides on which implementation to actually use based on the API client.
class Query(AsyncSyncMixin, async_impl=AsyncQueryImpl, synced_impl=SyncedQueryImpl):
    pass


# Backward compatibility: the class name as it was used in pre-async time.
# Auto-decides on which implementation to actually use based on the API client.
class WatchQuery(AsyncSyncMixin, async_impl=AsyncWatchQueryImpl, synced_impl=SyncedWatchQueryImpl):
    pass


def as_selector(value):
    if isinstance(value, str):
        return value
    s = []
    for k, v in value.items():
        bits = k.split("__")
        assert len(bits) <= 2, "too many __ in selector"
        if len(bits) == 1:
            label = bits[0]
            op = "eq"
        else:
            label = bits[0]
            op = bits[1]
        # map operator to selector
        if op == "eq":
            s.append("{}={}".format(label, v))
        elif op == "neq":
            s.append("{} != {}".format(label, v))
        elif op == "in":
            s.append("{} in ({})".format(label, ",".join(sorted(v))))
        elif op == "notin":
            s.append("{} notin ({})".format(label, ",".join(sorted(v))))
        else:
            raise ValueError("{} is not a valid comparison operator".format(op))
    return ",".join(s)
