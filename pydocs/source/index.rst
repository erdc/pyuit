***************
UITPy |version|
***************


Python wrapper for DoD HPCMP UIT+ rest interface"

Contents
========

.. toctree::
   :maxdepth: 1

   modules


Example Usage
=============

.. code-block:: python
   :caption: Example.py
   :name: example-py
   :linenos:

   # Initialize PbsScript
   pbs_script = PbsScript(
      job_name=”JOB_NAME”,
      project_id=”PROJECT_ID”,
      num_nodes=5,
      processes_per_node=36,
      max_time=timedelta(hours=8),
      queue=”debug”,
   )

   # Set optional directives
   pbs_script.set_directive(“-j”, “oe”)
   pbs_script.set_directive(“-A”, “ADH”)

   # Set modules
   pbs_script.load_module(“anaconda”)
   pbs_scipt.unload_module(“C++”)
   pbs_script.swap(“IntelMPI”, “OpenMPI”)

   # Set execution block
   pbs_script.execution_block = open(“script.sh”, “r”).read()

   # Create client
   client = Client(token=”TOKEN”)

   # Put files needed by pbs script on the system
   client.put_file(“/loca/path”. “/remote/path”)

   # Submit the script
   client.submit(pbs_script)

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
