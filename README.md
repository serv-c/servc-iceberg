# servc-iceberg

Iceberg extension for the servc library. Includes a managed interface for iceberg that includes schema migration, intialization and common query functions.

## Requirements

As per usual, this package does not come bundled with any libraries to ensure full flexibility on dependencies and security vulnerabilities.

```
$ pip install servc servc-iceberg pyiceberg
```

## Environment Variables

**DATA_PATH** - the location to start writing files. Default: /tmp/
**CATALOGUE_NAME** - the name of the catalogue to use. default: 'default'
**CATALOGUE_NAMESPACE** - the name of the namespace to use. default: 'namespace'
**CATALOGUE_TYPE** - the type of catalog. default: sql
**CATALOGUE_URI** - the postgres url for the catalog. eg: postgresql+psycopg2://username:password@localhost/mydatabase

## Documentation

Servc's documentation can be found https://docs.servc.ca

## Example

Here is the most simple example of use, 

```python
from servc.com.server.server import start_server

def inputProcessor(messageId, bus, cache, components, message, emit):
  pass

# the method 'methodA' will be resolved by inputProcessor
start_server(
  "my-route",
  {
    "methodA": inputProcessor
  }
)
```
