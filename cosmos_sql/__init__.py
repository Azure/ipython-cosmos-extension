import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class
import pandas as pd
import logging

CosmosClient = None
database = None
container = None
result_auto_convert_to_df = True

def load_ipython_extension(ipython):
    ipython.register_magics(CosmosMagics)

def unload_ipython_extension(ipython):
    CosmosClient = None
    pass

@magics_class
class CosmosMagics(Magics):
    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--database', '-d',
      help='specifies database name'
    )
    @magic_arguments.argument('--container', '-c',
      help='specifies container name'
    )
    @magic_arguments.argument('--asJson',
      help='specifies the output format'
    )
    def sql(self, line='', cell=None):
        global database, container, CosmosClient, result_auto_convert_to_df
        args = magic_arguments.parse_argstring(self.sql, line)
        self.ensure_connected()

        if args.database:
            database_id = args.database
        else:
            database_id = database
        if args.container:
            container_id = args.container
        else:
            container_id = container

        if database_id is None:
            raise Exception('database is not specified')

        if container_id is None:
            raise Exception('container is not specified')

        database_link = 'dbs/' + database_id
        earthquakes = database_link + '/colls/' + container_id
        query = {"query": cell}
        items = list(CosmosClient.QueryItems(earthquakes, query, {'enableCrossPartitionQuery': True}))
        if result_auto_convert_to_df:
            return pd.DataFrame.from_records(items)
        else:
            return items

    def ensure_connected(self):
        global CosmosClient

        if not CosmosClient:
            import os
            host = os.environ["COSMOS_ENDPOINT"]
            key = os.environ["COSMOS_KEY"]

            if (not host) or (not key):
                logging.error("Cosmos endpoint credentials is not set.")
                print("cosmos endpoint credentials is not set")
                raise Exception("cosmos endpoint credentials are not set")

            CosmosClient = cosmos_client.CosmosClient(host, {'masterKey': key})

    @line_magic("database")
    def set_database(self, line, cell="", local_ns=None):
        """ Sets database name
        Usage:
        * ``database database_name``  - sets database name
        """
        if not line:
            raise Exception('database is not specified')

        global database, container, CosmosClient
        database = line

    @line_magic("container")
    def set_container(self, line, cell="", local_ns=None):
        """ Sets container name
        Usage:
        * ``container container_name``  - sets container name
        """
        if not line:
            raise Exception('container is not specified')
        global database, container, CosmosClient
        container = line

    @line_magic("enable_autoconvert_to_dataframe")
    def enable_autoconvert_to_dataframe(self, line, cell="", local_ns=None):
        """ Enables auto conversion of the result to dataframe
        Usage:
        * ``%enable_autoconvert_to_dataframe`` - Enable automatically convert the result to dataframe.
        """
        global result_auto_convert_to_df
        result_auto_convert_to_df = True

    @line_magic("disable_autoconvert_to_dataframe")
    def disable_autoconvert_to_dataframe(self, line, cell="", local_ns=None):
        """ Disables auto conversion of the result to dataframe
        Usage:
        * ``%disable_autoconvert_to_dataframe`` - Disable automatically convert the result to dataframe.
        """
        global result_auto_convert_to_df
        result_auto_convert_to_df = False