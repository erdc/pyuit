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

1. Pass in the keys using the ``client_id`` and ``client_secret`` key word arguments to the ``Client`` constructor::

  from uit import Client

  c = Client(client_id='<YOUR_CLIENT_ID>', client_secret='<YOUR_CLIENT_SECRET>')

2. Set the ``UIT_ID`` and ``UIT_SECRET`` environmental variables

If the ``client_id`` or the ``client_secrete`` are not passed into the ``Client`` when it is instantiated, then it will next look for those values from environmental variables. On UNIX systems you can set the environmental variables as follows::

  export UIT_ID="<YOUR_CLIENT_ID>"
  export UIT_SECRET="<YOUR_CLIENT_SECRET>"

3. Store the keys in a :file:`~/.uit` configuration file

If the ``Client`` does not receive they ``Client ID`` and ``Client Secret`` keys from either the key word arguments or environmental variables it will finally look in a configuration file. By default it will look for a configuration file in the user's home directory called :file:`.uit`. The file should look like this::

  client_id: <YOUR_CLIENT_ID>
  client_secret: <YOUR_CLIENT_SECRET>

If you have a configuration file located somewhere other than :file:`~/.uit` then you need to pass the path to the file into the ``Client`` constructor::

  c = Client(config_file="/path/to/your/uit/config/file")


**CONDA BUILD**

conda build -c erdc -c conda-forge conda.recipe