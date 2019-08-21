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

from IPython.core.display import display
from IPython.core.display import HTML
from IPython import get_ipython
import sys

class IpythonDisplay(object):
    def __init__(self):
        self._ipython_shell = get_ipython()

    def html(self, to_display):
        display(HTML(to_display))

    def display(self, msg):
        self._ipython_shell.write(msg)
        self.__stdout_flush()

    def display_error(self, error):
        self._ipython_shell.write_err(u"{}\n".format(error))
        self.__stderr_flush()

    def __stdout_flush(self):
        sys.stdout.flush()

    def __stderr_flush(self):
        sys.stdout.flush()

@magics_class
class CosmosMagics(Magics):
    def __init__(self, shell, data=None):
        super(CosmosMagics, self).__init__(shell)
        self._ipython_display = IpythonDisplay()
        self.ensure_connected()

    @line_magic("sql")
    def sql_help(self, line, cell="", local_ns=None):
        """ displays help
        """
        self.help_internal()

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
        if args.help:
            self.help_internal()
            return

        global database, container, CosmosClient, result_auto_convert_to_df
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
            self.shell.user_ns['cosmos_client'] = CosmosClient

    @line_magic("database")
    def set_database(self, line, cell="", local_ns=None):
        """ Sets the default Cosmos database to be used in queries.
        Usage:
        * ``database DATABASE_NAME``  - sets database name
        """
        if not line:
            raise Exception('database is not specified')

        global database, container, CosmosClient
        database = line

    @line_magic("container")
    def set_container(self, line, cell="", local_ns=None):
        """ Sets the default Cosmos container to be used in queries.
        Usage:
        * ``container CONTAINER_NAME``  - sets container name
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

    @line_magic
    def cosmos_help(self, line, cell="", local_ns=None):
        self.help_internal()

    def help_internal(self):
        """ Displays help
        Usage:
        * ``help``  - displays help
        """
        help_html = u"""
The following provides the guide for Cosmos magic functions:
<table>
  <tr style="text-align:left;">
    <th style="text-align:left;">Magic</th>
    <th style="text-align:left;">Example</th>
    <th style="text-align:left;">Description</th>
  </tr>  
  <tr style="text-align:left;">
    <td style="text-align:left;">sql</td>
    <td style="text-align:left;">%%sql --database databaseName --container containerName
                            <br/>SELECT top 1 r.id, r._ts from r order by r._ts desc</td>
    <td style="text-align:left;">Queries Azure Cosmos DB using the given Cosmos database and container.
    Parameters:
      <ul>
        <li>--database DATABASE_NAME: If provided, this Cosmos database will be used;
         otherwise the default database will be used.</li>
        <li>--container CONTAINER_NAME: If provided, this Cosmos container will be used; 
        otherwise the default container will be used.</li>
        <li>--output VAR_NAME: The dataframe of the result will be stored in a variable with this name.</li>
      </ul>
      Learn about the Cosmos query language: https://aka.ms/CosmosQuery
    </td>
  </tr>
  <tr style="text-align:left;">
    <td style="text-align:left;">database</td>
    <td style="text-align:left;">%database databaseName</td>
    <td style="text-align:left;">Sets the default Cosmos database to be used in queries.</td>
  </tr>
  <tr style="text-align:left;">
    <td style="text-align:left;">container</td>
    <td style="text-align:left;">%container containerName</td>
    <td style="text-align:left;">Sets the default Cosmos container to be used in queries.</td>
  </tr>
</table>
"""
        self._ipython_display.html(help_html)
