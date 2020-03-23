import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmosclient_builder
import azure.cosmos.errors as errors
from IPython.core import magic_arguments
from IPython.core.magic import line_magic, cell_magic, line_cell_magic, Magics, magics_class
import pandas as pd
import logging

cosmos_client = None
database = None
container = None
result_auto_convert_to_df = True

def load_ipython_extension(ipython):
    ipython.register_magics(CosmosMagics)

def unload_ipython_extension(ipython):
    cosmos_client = None
    pass

@magics_class
class CosmosMagics(Magics):
    def __init__(self, shell, data=None):
        super(CosmosMagics, self).__init__(shell)
        self.ensure_connected()

    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--database', '-d', type=str, default=None,
      help='If provided, this Cosmos database will be used;'
    )
    @magic_arguments.argument('--container', '-c', type=str, default=None,
      help='If provided, this Cosmos container will be used;'
    )
    @magic_arguments.argument('--output',
      help='The dataframe of the result will be stored in a variable with this name.', type=str, default=None,
    )
    def sql(self, line, cell="", local_ns=None):
        """
        Queries Azure Cosmos DB using the given Cosmos database and container.
        Learn about the Cosmos query language: https://aka.ms/CosmosQuery

        Example:
            %%sql --database databaseName --container containerName
            SELECT top 1 r.id, r._ts from r order by r._ts desc
        """
        args = magic_arguments.parse_argstring(self.sql, line)

        global database, container, cosmos_client, result_auto_convert_to_df
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

        query = cell
        container = cosmos_client.get_database_client(database_id).get_container_client(container_id)

        query_iterable = container.query_items(
            query=query,
            enable_cross_partition_query=True
        )

        items = list(query_iterable)

        if result_auto_convert_to_df:
            result = self.to_data_frame(items)
        else:
            result = items

        if args.output is not None:
            self.shell.user_ns[args.output] = result
            return None
        else:
            return result

    def to_data_frame(self, items):
        try:
            return pd.DataFrame.from_records(items)
        except TypeError:
            return pd.DataFrame.from_dict(items)

    def ensure_connected(self):
        global cosmos_client

        if not cosmos_client:
            import os
            host = os.environ["COSMOS_ENDPOINT"]
            key = os.environ["COSMOS_KEY"]

            if (not host) or (not key):
                logging.error("Cosmos endpoint credentials is not set.")
                print("cosmos endpoint credentials is not set")
                raise Exception("cosmos endpoint credentials are not set")

            cosmos_client = cosmosclient_builder.CosmosClient(host, {'masterKey': key},
                                                              user_agent='CosmosNotebookServicePythonClient')
            self.shell.user_ns['cosmos_client'] = cosmos_client

    @line_magic("database")
    def set_database(self, line, cell="", local_ns=None):
        """ Sets the default Cosmos database to be used in queries.
        Usage:
            %database DATABASE_NAME
        """
        if not line:
            raise Exception('database is not specified')

        global database, container, cosmos_client
        # remove empty spaces
        database = line.strip()

    @line_magic("container")
    def set_container(self, line, cell="", local_ns=None):
        """ Sets the default Cosmos container to be used in queries.
        Usage:
            %container CONTAINER_NAME
        """
        if not line:
            raise Exception('container is not specified')
        global database, container, cosmos_client
        # remove empty spaces
        container = line.strip()

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