"""
A class-factoring logic for sync & async compatibility,
"""
import asyncio
import functools
import inspect
import queue
from concurrent.futures import ThreadPoolExecutor
from typing import Type, Optional

threadpool = ThreadPoolExecutor(thread_name_prefix='pykube')

# A marker for queue of async-generators, when the generator has exited.
FIN = object()


async def _stream2queue(async_genfn, args, kwargs, queue, fin=FIN):
    try:
        async for item in async_genfn(*args, **kwargs):
            queue.put(item)  # sync & blocking!
    except Exception as e:
        raise
    else:
        pass
    finally:
        queue.put(fin)  # sync & blocking!


def _thread_fn(async_fn, args, kwargs):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(async_fn(*args, **kwargs))
    except Exception as e:
        raise
    else:
        return result
    finally:
        pass
        # loop.run_until_complete(loop.shutdown_asyncgens())
        # loop.close()


def synced_genfn(async_genfn, *, loop: Optional[asyncio.AbstractEventLoop] = None):
    @functools.wraps(async_genfn)
    def synced_genfn(*args, **kwargs):
        gen_queue = queue.Queue()
        if loop is not None:
            future = asyncio.run_coroutine_threadsafe(
                coro=_stream2queue(async_genfn, args, kwargs, gen_queue, FIN),
                loop=loop)
        else:
            future = threadpool.submit(
                _thread_fn,
                _stream2queue,
                (async_genfn, args, kwargs, gen_queue, FIN),
                {})
        try:
            while True:  # TODO: not feature.ready?()
                item = gen_queue.get()
                if item is FIN:
                    break
                else:
                    yield item
        finally:
            future.result()  # re-raise
    return synced_genfn


def synced_fn(async_fn, *, loop: Optional[asyncio.AbstractEventLoop] = None):
    @functools.wraps(async_fn)
    def synced_fn(*args, **kwargs):
        if loop is not None:
            future = asyncio.run_coroutine_threadsafe(
                coro=async_fn(*args, **kwargs),
                loop=loop)
            return future.result()  # block, wait, reraise
        else:
            future = threadpool.submit(_thread_fn, async_fn, args, kwargs)
            return future.result()
    return synced_fn


def synced_coroutine(coro, *, loop: Optional[asyncio.AbstractEventLoop] = None):
    if loop is not None:
        future = asyncio.run_coroutine_threadsafe(
            coro=coro,
            loop=loop)
        return future.result()  # block, wait, reraise
    else:
        future = threadpool.submit(_thread_fn, async_fn, args, kwargs)
        return future.result()


class SyncWrapper:
    """
    Turns a class into a synchronous implementation of a mainly async class.

    All async methods are converted into the same-named sync methods, with
    the original async method being executed in a local (per-call) event-loop.
    See `synced_fn` & `synced_genfn` for async-to-sync convertion details.

    Internally, it wraps the async instance and proxies all attributes to it,
    with async methods converted to the sync ones on the fly.
    There is no pre-creation of each method at the moment (for performance).

    This does not apply to the sub-classes. If a subclass of the sync class
    is created, it will wrap an instance of the original async class. No new
    classes are dynamically created on this.

    Usage::

        class SyncImpl(SyncWrapper, async_cls=AsyncImpl):
            pass
    """
    async_cls = None

    def __init_subclass__(cls, async_cls=None, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.async_cls = async_cls if async_cls is not None else getattr(cls, '_async_cls', getattr(cls, 'async_cls', None))

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.__loop = kwargs.get('loop')
        if self.__loop is None and args and isinstance(args[0], SyncWrapper):
            self.__loop = args[0].__loop
        self.__wrapped = type(self).async_cls(*args, **kwargs)

    def __getattr__(self, name):
        # TODO: if we have a loop, use that loop explicitly!
        loop = self.__loop
        result = getattr(self.__wrapped, name)
        if inspect.iscoroutine(result):
            result = synced_coroutine(result, loop=loop)
        elif inspect.isasyncgenfunction(result):
            result = synced_genfn(result, loop=loop)
        elif inspect.iscoroutinefunction(result):
            result = synced_fn(result, loop=loop)
        return result

    def __delattr__(self, name):
        if name.startswith('__') or name == '_wrapped' or '__' in name:
            super().__delattr__(name)
        else:
            delattr(self.__wrapped, name)

    def __setattr__(self, name, value):
        if name.startswith('__') or name == '_wrapped' or '__' in name:
            super().__setattr__(name, value)
        else:
            setattr(self.__wrapped, name, value)

    @property
    def async_wrapped(self):
        # TODO: Used in queries to get async api. Can we get rid of this?
        return self.__wrapped


class AsyncSyncMixin(object):
    """
    Turns a class into a factory of either sync or async actual classes.
    corresponding to a type of the first construction arguments (``api``).

    Internally, for performance, it dynamically constructs the classes from
    the newly declared class, and adds the sync/async implementations
    as the base classes.

    When instantiated, one of these derived classes is actually used.
    This guarantees that all instances belong to the declared class,
    so that ``isinstance()`` checks return the expected results.

    Usage::

        class My(AsyncSyncMixin, async_impl=AsyncImpl, synced_impl=SyncedImpl):
            pass
    """
    _synced_cls = None
    _async_cls = None

    def __init_subclass__(cls: type, async_impl=None, synced_impl=None, **kwargs):
        super().__init_subclass__(**kwargs)
        async_impl = async_impl if async_impl is not None else getattr(cls, '_async_impl', None)
        synced_impl = synced_impl if synced_impl is not None else getattr(cls, '_synced_impl', None)
        if async_impl is not None or synced_impl is not None:
            if async_impl in cls.__mro__ or synced_impl in cls.__mro__:
                return

            async_cls_name = f'async_{cls.__name__}'
            synced_cls_name = f'synced_{cls.__name__}'
            cls._async_impl = async_impl
            cls._synced_impl = synced_impl
            cls._async_cls = type(async_cls_name, (cls, async_impl), {})
            cls._synced_cls = type(synced_cls_name, (cls, synced_impl), {})

            # TODO: remake to metaclasses, and provide the cls.base/cls.version there
            for name in ['base', 'version', 'endpoint', 'plural']:
                if hasattr(cls._async_cls, name):
                    setattr(cls, name, getattr(cls._async_cls, name))

    def __new__(cls: Type['AsyncSyncMixin'], api, *args, **kwargs):
        from .http import AsyncHTTPClient, SyncedHTTPClient

        # If the actual class is one of the sync/async ones, we go normally.
        # Otherwise, guess by the `api` argument, and use specific sync/async class.
        if cls._synced_cls is None or cls._async_cls is None:
            return super().__new__(cls, api, *args, **kwargs)
        if issubclass(cls, (cls._synced_cls, cls._async_cls)):
            return super().__new__(cls)
            # return super().__new__(cls, api, *args, **kwargs)
        elif isinstance(api, AsyncHTTPClient):
            return super().__new__(cls._async_cls)
            # return super().__new__(cls._async_cls, api, *args, **kwargs)
        elif isinstance(api, SyncedHTTPClient):
            return super().__new__(cls._synced_cls)
            # TODO return super().__new__(cls._synced_cls, api, *args, **kwargs)
        else:
            raise TypeError(f"`api` must be an async/sync HTTP client; got {api!r}")
