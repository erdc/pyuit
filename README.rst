===============================
uit
===============================


.. image:: https://img.shields.io/travis/erdc/pyuit.svg
        :target: https://travis-ci.com/erdc/pyuit


Python wrapper for DoD HPCMP UIT+ rest interface

**INSTALL**

conda install::

    conda install -c erdc/label/dev -c conda-forge pyuit

development install::

    conda env create -f environment.yml
    conda activate uit
    python setup.py develop



**CONDA BUILD**

conda build -c erdc -c conda-forge conda.recipe