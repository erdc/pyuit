=====
PyUIT
=====


.. image:: https://img.shields.io/travis/erdc/pyuit.svg
        :target: https://travis-ci.com/erdc/pyuit


Python wrapper for DoD HPCMP UIT+ REST interface

**INSTALL**

conda install::

    conda install -c erdc/label/dev -c conda-forge pyuit

development install::

    git clone https://github.com/erdc/pyuit.git
    cd pyuit
    conda env create -f environment.yml
    conda activate uit
    pip install -e .


**CONFIGURE**

For PyUIT to communicate with the UITP+ REST API you must first register a client application on the `UIT+ site <https://www.uitplus.hpc.mil/uapi/dash_clients>`_ (refer to the `UIT+ docs <https://www.uitplus.hpc.mil/files/README.pdf>`_ for more details). When registering the client application you should select `Web Application` for "Client Type", and use the following URL as the "Return URL"::

  http://localhost:5000/save_token

Once you have registered a client application you should have a ``Client ID`` and a ``Client Secret``. You need to use these two keys to configure PyUIT locally.

There are three ways that you can configure PyUIT to use the ``Client ID`` and the ``Client Secret``. PyUIT search for these keys in this order:

1. Pass in the keys using the ``client_id`` and ``client_secret`` key word arguments to the ``Client`` constructor

::

  from uit import Client

  c = Client(client_id='<YOUR_CLIENT_ID>', client_secret='<YOUR_CLIENT_SECRET>')

2. Set the ``UIT_ID`` and ``UIT_SECRET`` environmental variables

If the ``client_id`` or the ``client_secret`` are not passed into the ``Client`` when it is instantiated, then it will next look for those values from environmental variables. On UNIX systems you can set the environmental variables as follows::

  export UIT_ID="<YOUR_CLIENT_ID>"
  export UIT_SECRET="<YOUR_CLIENT_SECRET>"

3. Store the keys in a :file:`~/.uit` configuration file

If the ``Client`` does not receive they ``Client ID`` and ``Client Secret`` keys from either the key word arguments or environmental variables it will finally look in a configuration file. By default it will look for a configuration file in the user's home directory called :file:`.uit`. The file should look like this::

  client_id: <YOUR_CLIENT_ID>
  client_secret: <YOUR_CLIENT_SECRET>

If you have a configuration file located somewhere other than :file:`~/.uit` then you either need to specify the path in the `UIT_CONFIG_FILE` environment variable or pass the path to the file into the ``Client`` constructor::

  export UIT_CONFIG_FILE="/path/to/your/uit/config/file"
  c = Client(config_file="/path/to/your/uit/config/file")

PyUIT comes with a default specification of the nodes types for the DSRC HPC systems which should work in most situations. In the rare event that the default is not sufficient, it can be overridden by specifying a custom file in the :file:`~/.uit` configuration file or by defining a `UIT_NODE_TYPES_FILE` environment variable::

  node_types_file: /path/to/your/node_types/file
  export UIT_NODE_TYPES_FILE="/path/to/your/node_types/file"

The file should be a CSV file with a header row specifying the system and then the node types and value rows specifying the system and then number of cores on each node type. For example::

  system,compute,gpu,bigmem,transfer,mla,highclock
  nautilus,128,128,128,1,128,32

The ``dodcerts`` library is used to provide the default Certificate Authority bundle. If a custom certificate bundle file is required it can be specified in the :file:`~/.uit` configuration file or by using the ``UIT_CA_FILE`` environment variable. It can also be passed into the ``Client`` constructor::

  ca_file: /path/to/custom/dod/ca/bundle
  export UIT_CA_FILE="/path/to/custom/dod/ca/bundle"
  c = Client(ca_file="/path/to/custom/dod/ca/bundle")

**CONDA BUILD**

conda build -c erdc -c conda-forge conda.recipe