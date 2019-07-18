"""
HTTP request related code.
"""
import abc
import asyncio
import posixpath
import ssl
import threading
from typing import Optional

import aiohttp

try:
    import google.auth
    from google.auth.transport.requests import Request as GoogleAuthRequest
    google_auth_installed = True
except ImportError:
    google_auth_installed = False

from urllib.parse import urlparse

from ._syncasync import SyncWrapper
from .exceptions import HTTPError
from .config import KubeConfig

DEFAULT_HTTP_TIMEOUT = 10*60  # seconds


class AsyncHTTPClient:

    def __init__(self, config: KubeConfig, timeout: float = DEFAULT_HTTP_TIMEOUT):
        """
        Creates a new instance of a client.

        :Parameters:
           - `config`: The configuration instance
        """
        super().__init__()
        self.config = config
        self.timeout = timeout
        self.url = self.config.cluster["server"]

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        pr = urlparse(value)
        self._url = pr.geturl()

    def get_kwargs(self, **kwargs) -> dict:
        """
        Creates a full URL to request based on arguments.

        :Parametes:
           - `kwargs`: All keyword arguments to build a kubernetes API endpoint
        """
        version = kwargs.pop("version", "v1")
        if version == "v1":
            base = kwargs.pop("base", "/api")
        elif "/" in version:
            base = kwargs.pop("base", "/apis")
        else:
            if "base" not in kwargs:
                raise TypeError("unknown API version; base kwarg must be specified.")
            base = kwargs.pop("base")
        bits = [base, version]
        # Overwrite (default) namespace from context if it was set
        if "namespace" in kwargs:
            n = kwargs.pop("namespace")
            if n is not None:
                if n:
                    namespace = n
                else:
                    namespace = self.config.namespace
                if namespace:
                    bits.extend([
                        "namespaces",
                        namespace,
                    ])
        url = kwargs.get("url", "")
        if url.startswith("/"):
            url = url[1:]
        bits.append(url)
        kwargs["url"] = self.url + posixpath.join(*bits)
        if 'timeout' not in kwargs:
            # apply default HTTP timeout
            kwargs['timeout'] = self.timeout
        if 'data' in kwargs:
            kwargs.setdefault('headers', {}).setdefault('Content-Type', 'application/json')
        return kwargs

    # TODO: put all abstract async-def methods here, just to have them.
    @abc.abstractmethod
    async def get(self, *args, **kwargs):
        pass


class SyncedHTTPClient(SyncWrapper, async_cls=AsyncHTTPClient):

    def __init__(self, *args, **kwargs):
        # if not isinstance(api, AsyncHTTPClient):
        #     raise TypeError(f"Async client is expected as input; got {api!r}")

        self.__lock = threading.Lock()
        self.__loop = asyncio.new_event_loop()
        self.__stop = asyncio.Event(loop=self.__loop)
        self.__thread = threading.Thread(
            name=f'pykube-{id(self)}',
            target=self.__thread_for_loop,
            kwargs=dict(loop=self.__loop, stop=self.__stop))

        kwargs['loop'] = self.__loop
        super().__init__(*args, **kwargs)
        self.__start_thread()  # now or maybe lazily later

    def __del__(self):
        self._stop_thread()

    def __start_thread(self):
        # TODO: thread spawning is expensive. clients are re-created often. use a thread pool here.
        with self.__lock:
            if self.__thread is not None and not self.__thread.is_alive():
                self.__thread.start()

    def _stop_thread(self):
        if self.__thread is not None and self.__thread.is_alive():
            self.__loop.call_soon_threadsafe(self.__stop.set)
            self.__thread.join()

    # @staticmethod
    def __thread_for_loop(self, loop: asyncio.AbstractEventLoop, stop: asyncio.Event):
        # Assume that a fresh thread has no loop running already.
        asyncio.set_event_loop(loop)
        loop.run_until_complete(stop.wait())


class AioHTTPClient(AsyncHTTPClient):
    """
    Client for interfacing with the Kubernetes API.
    """

    def __init__(self,
                 config: KubeConfig,
                 timeout: float = DEFAULT_HTTP_TIMEOUT,
                 *,
                 loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Creates a new instance of the HTTPClient.

        :Parameters:
           - `config`: The configuration instance
        """
        super().__init__(config=config, timeout=timeout)

        # One client should fully run in one event-loop, with one conn-pool.
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._sesion_future = asyncio.Future(loop=self._loop)

        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
        loop.call_soon_threadsafe(self._lazy_session, self._sesion_future, config)

    @property
    async def version(self):
        """
        Get Kubernetes API version
        """
        response = await self.get(version="", base="/version")
        response.raise_for_status()
        data = await response.json()
        return (data["major"], data["minor"])

    async def resource_list(self, api_version):
        cached_attr = f'_cached_resource_list_{api_version}'
        if not hasattr(self, cached_attr):
            r = await self.get(version=api_version)
            r.raise_for_status()
            setattr(self, cached_attr, await r.json())
        return getattr(self, cached_attr)

    async def raise_for_status(self, resp):
        try:
            resp.raise_for_status()
        except Exception as e:
            # attempt to provide a more specific exception based around what
            # Kubernetes returned as the error.
            if resp.headers["content-type"] == "application/json":
                payload = await resp.json()
                if payload["kind"] == "Status":
                    raise HTTPError(resp.status_code, payload["message"])
            raise

    async def request(self, *args, **kwargs):
        """
        Makes an API request based on arguments.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.request(*args, **self.get_kwargs(**kwargs))

    async def get(self, *args, **kwargs):
        """
        Executes an HTTP GET.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.get(*args, **self.get_kwargs(**kwargs))

    async def options(self, *args, **kwargs):
        """
        Executes an HTTP OPTIONS.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.options(*args, **self.get_kwargs(**kwargs))

    async def head(self, *args, **kwargs):
        """
        Executes an HTTP HEAD.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.head(*args, **self.get_kwargs(**kwargs))

    async def post(self, *args, **kwargs):
        """
        Executes an HTTP POST.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.post(*args, **self.get_kwargs(**kwargs))

    async def put(self, *args, **kwargs):
        """
        Executes an HTTP PUT.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.put(*args, **self.get_kwargs(**kwargs))

    async def patch(self, *args, **kwargs):
        """
        Executes an HTTP PATCH.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.patch(*args, **self.get_kwargs(**kwargs))

    async def delete(self, *args, **kwargs):
        """
        Executes an HTTP DELETE.

        :Parameters:
           - `args`: Non-keyword arguments
           - `kwargs`: Keyword arguments
        """
        session = await self._sesion_future
        return await session.delete(*args, **self.get_kwargs(**kwargs))

    @staticmethod
    def _lazy_session(future: asyncio.Future, config: KubeConfig):
        """
        Create an initialise an `aiohttp.ClientSession` for the client.

        Objects of `aiohttp` must be created inside of a running loop.
        And closed there too.

        But objects of this class can be created outside of the loop,
        despite the loop is provided to them as an argument.

        So, we schedule the session creation as soon as the client is created,
        or slightly after that -- as soon as the loop takes the next iteration.

        All requesting methods (get/post/patch/delete/etc) first wait
        for the session to be defined (via a session future),
        and use it only after that. It usually happens instantly.
        """
        conn = None
        cafile = None
        headers = {}

        if "certificate-authority" in config.cluster:
            cafile = config.cluster["certificate-authority"].filename()
        # elif "insecure-skip-tls-verify" in self.config.cluster:
        #     kwargs["verify"] = not config.cluster["insecure-skip-tls-verify"]

        if "token" in config.user and config.user["token"]:
            headers["Authorization"] = "Bearer {}".format(config.user["token"])
        elif "client-certificate" in config.user:
            cert_file = config.user["client-certificate"].filename()
            client_key = config.user["client-key"].filename()
            sslcontext = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH, cafile=cafile)
            sslcontext.load_cert_chain(certfile=cert_file, keyfile=client_key)
            conn = aiohttp.TCPConnector(ssl_context=sslcontext, limit=0)

        session = aiohttp.ClientSession(connector=conn, headers=headers)
        future.set_result(session)


# Backward compatibility: a synchronous (actually, synchronised) client with
# some default asynchronous implementation under the hood (any is suitable).
class HTTPClient(SyncedHTTPClient, async_cls=AioHTTPClient):
    pass
