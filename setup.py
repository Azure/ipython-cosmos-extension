from distutils.core import setup

version = open('cosmos_sql/VERSION', 'r').readline().strip()

long_desc = """
IPython extension for Cosmos SQL Query Language
"""

setup(
    name='ipython-cosmos-extension',
    version=version,
    packages=['cosmos_sql'],
    url='https://github.com/moderakh/ipython-cosmos-extension',
    license='MIT',
    author='moderakh',
    author_email='moderakh@users.noreply.github.com',
    description='IPython Extension for Cosmos SQL integration',
    install_requires=["azure-cosmos"]
)
