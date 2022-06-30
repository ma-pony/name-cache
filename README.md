# nameko-cache

An easy-to-use nameko cache

Some code references from repo [nameko-cachetools](https://github.com/santiycr/nameko-cachetools)

# Install
```shell
pip install nameko-cachelib
```

# Example
```python

    class Service:
        name = "service"

        cached_service = CacheRpcProxy("service")

        @rpc
        def cached(self, *args, **kwargs):
            return self.cached_service.some_method(*args, **kwargs)

```

# TODO list
## memory cache

## single flight

## redis cache


