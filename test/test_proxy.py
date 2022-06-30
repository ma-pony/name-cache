import time

import pytest
from mock import Mock
from nameko.rpc import rpc
from nameko.testing.services import entrypoint_hook
from proxy import CacheRpcProxy


@pytest.fixture
def mock_func():
    mock_func = Mock()
    return mock_func


@pytest.fixture
def container(container_factory, rabbit_config, mock_func):
    class Service:
        name = "service"

        cached_service = CacheRpcProxy("service")

        @rpc
        def cached(self, *args, **kwargs):
            return self.cached_service.some_method(*args, **kwargs)

        @rpc
        def some_method(self, *args, **kwargs):
            mock_func()
            time.sleep(3)
            return "method"

        @rpc
        def error_cached(self, *args, **kwargs):
            return self.cached_service.some_error_method(*args, **kwargs)

        @rpc
        def some_error_method(self, *args, **kwargs):
            raise Exception("error_method")

    container = container_factory(Service, rabbit_config)
    container.start()

    return container


def test_cached_response(container, mock_func):

    with entrypoint_hook(container, "cached") as hook:
        assert hook("test") == "method"
        assert hook("test") == "method"
        assert hook("test") == "method"
        assert hook("test") == "method"
        assert hook("test") == "method"
        assert mock_func.call_count == 1
