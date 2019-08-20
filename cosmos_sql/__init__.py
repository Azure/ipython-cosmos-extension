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

    @cell_magic
    @magic_arguments.magic_arguments()
    @magic_arguments.argument('--database', '-d', type=str, default=None,
      help='specifies database name'
    )
    @magic_arguments.argument('--container', '-c', type=str, default=None,
      help='specifies container name'
    )
    @magic_arguments.argument('--asJson',
      help='specifies the output format'
    )
    @magic_arguments.argument('--output',
      help='specifies the output variable name', type=str, default=None,
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

    @line_magic
    def help(self, line, cell="", local_ns=None):
        """ Displays help
        Usage:
        * ``help``  - displays help
        """
        help_html = u"""
The following provides the cosmos SQL magic function guide:
<table>
  <tr>
    <th>Magic</th>
    <th>Example</th>
    <th>Description</th>
  </tr>  
  <tr>
    <td>database</td>
    <td>%database dbName</td>
    <td>sets the default cosmos database to be used in queries.</td>
  </tr>
  <tr>
    <td>container</td>
    <td>%container containerName</td>
    <td>sets the default cosmos container to be used in queries.</td>
  </tr>
  <tr>
    <td>sql</td>
    <td>%%sql --database dbName --container containerName<br/>SELECT top 1 r.id, r._ts from r order by r._ts desc</td>
    <td>Queries cosmos using the given cosmos database and container.
    Parameters:
      <ul>
        <li>--database if provided this cosmos database will be used otherwise the default cosmos database will be used.</li>
        <li>--container if provided this cosmos container will be used otherwise the default cosmos container will be used.</li>
        <li>--output VAR_NAME: the result data-frame will be stored in the given variable name.</li>
      </ul>
    </td>
  </tr>
</table>
"""
        self._ipython_display.html(help_html)