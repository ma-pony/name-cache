import json
import logging
import uuid
from functools import partial

from nameko.amqp import UndeliverableMessage
from nameko.exceptions import UnknownService, deserialize
from nameko.messaging import encode_to_headers
from nameko.rpc import Client, ClusterRpc, RpcCall

_log = logging.getLogger(__name__)


def cache_hash_key(args: tuple, kwargs: dict) -> int:
    return hash(json.dumps((args, kwargs), sort_keys=True))


class CacheRpcCall(RpcCall):
    _response = None

    def __init__(self, correlation_id, send_request, get_response, cache=None):
        self.correlation_id = correlation_id
        self._send_request = send_request
        self._get_response = get_response
        self.cache = cache
        self.cache_hash_key = None
        super(CacheRpcCall, self).__init__(correlation_id, send_request, get_response)

    def send_request(self, *args, **kwargs):
        """Send the RPC request to the remote service"""
        payload = {"args": args, "kwargs": kwargs}
        self._send_request(payload)

    def get_response(self):
        """Retrieve the response for this RPC call. Blocks if the response
        has not been received.
        """
        if self._response is not None:
            return self._response

        self._response = self._get_response()
        return self._response

    def result(self):
        """Return the result of this RPC call, blocking if the response
        has not been received.

        Raises a `RemoteError` if the remote service returned an error
        response.
        """

        response = self.get_response()
        self.cache[self.cache_hash_key] = response
        error = response.get("error")
        if error:
            raise deserialize(error)
        return response["result"]


class CacheClient(Client):
    cache_storage = {}

    def __getattr__(self, name):
        if self.method_name is not None:
            raise AttributeError(name)

        if self.service_name:
            target_service = self.service_name
            target_method = name
        else:
            target_service = name
            target_method = None

        clone = CacheClient(
            self.publish,
            self.register_for_reply,
            self.context_data,
            target_service,
            target_method,
        )
        return clone

    def get_from_cache(self, key: int | str):
        if key not in self.cache_storage:
            return False, None
        return True, self.cache_storage.get(key)

    def _call(self, *args: tuple, **kwargs: dict):
        if not self.fully_specified:
            raise ValueError(
                "Cannot call unspecified method {}".format(self.identifier)
            )

        _log.debug("invoking %s", self)

        # We use the `mandatory` flag in `producer.publish` below to catch rpc
        # calls to non-existent services, which would otherwise wait forever
        # for a reply that will never arrive.
        #
        # However, the basic.return ("no one is listening for topic") is sent
        # asynchronously and conditionally, so we can't wait() on the channel
        # for it (will wait forever on successful delivery).
        #
        # Instead, we make use of (the rabbitmq extension) confirm_publish
        # (https://www.rabbitmq.com/confirms.html), which _always_ sends a
        # reply down the channel. Moreover, in the case where no queues are
        # bound to the exchange (service unknown), the basic.return is sent
        # first, so by the time kombu returns (after waiting for the confim)
        # we can reliably check for returned messages.

        # Note that deactivating publish-confirms in the Client will disable
        # this functionality and therefore :class:`UnknownService` will never
        # be raised (and the caller will hang).

        correlation_id = str(uuid.uuid4())

        extra_headers = encode_to_headers(self.context_data)

        cache_key = cache_hash_key(args, kwargs)

        has_cache, cache_data = self.get_from_cache(cache_key)

        rpc_call = CacheRpcCall(correlation_id, None, None, self.cache_storage)

        if has_cache:
            get_response = lambda _: cache_data  # noqa E731
            rpc_call._get_response = get_response
            return rpc_call

        else:
            get_response = self.register_for_reply(correlation_id)

            send_request = partial(
                self.publish,
                routing_key=self.identifier,
                mandatory=True,
                correlation_id=correlation_id,
                extra_headers=extra_headers,
            )
            rpc_call._send_request = send_request
            rpc_call._get_response = get_response
            try:
                rpc_call.send_request(*args, **kwargs)
            except UndeliverableMessage:
                raise UnknownService(self.service_name)

            return rpc_call


class CacheStorage:
    def __init__(self, cache: dict = None):
        self.cache = cache if cache else {}

    """
    The self-defined cache storage class must implement the following methods
    """

    def clear(self):
        """D.clear() -> None.  Remove all items from D."""
        return self.cache.clear()

    def get(self, *args, **kwargs):
        """Return the value for key if key is in the dictionary,
        else default."""
        return self.cache.get(*args, **kwargs)

    def pop(self, k, d=None):
        """
        D.pop(k[,d]) -> v, remove specified key and
        return the corresponding value.
        If the key is not found, return the default if given; otherwise,
        raise a KeyError.
        """
        return self.cache.pop(k, d)

    def __contains__(self, *args, **kwargs):
        """True if the dictionary has the specified key,
        else False."""
        return self.cache.__contains__(*args, **kwargs)

    def __setitem__(self, *args, **kwargs):
        """Set self[key] to value."""
        return self.cache.__setitem__(*args, **kwargs)


class CacheRpcProxy(ClusterRpc):
    """DependencyProvider for injecting an RPC client
     to a specific serviceinto a service.

    As per :class:`~nameko.rpc.ClusterRpc` but with a pre-specified target
    service.

    :Parameters:
        target_service : str
            Target service name
    """

    def __init__(self, target_service, **publisher_options):
        self.target_service = target_service
        self.cache_storage: CacheStorage = publisher_options.pop("cache", dict())
        super(CacheRpcProxy, self).__init__(**publisher_options)

    def get_dependency(self, worker_ctx):
        publish = self.publisher.publish
        register_for_reply = self.reply_listener.register_for_reply

        client = CacheClient(publish, register_for_reply, worker_ctx.context_data)

        return getattr(client, self.target_service)
