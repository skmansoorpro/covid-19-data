COVID-19 dataset by Our World in Data
=====================================
At Our World in Data we have been collecting COVID-19 data from a variety of domains since the pandemic started. These
domains include testing, vaccinations, hospitalizations and more. We believed (and still believe) that to make progress
against the outbreak of the Coronavirus disease – COVID-19 – we need to understand how the pandemic is developing. And
for this, we need reliable and timely data. Therefore have focused our work on bringing together the research and
statistics on the COVID-19 outbreak.



How to use this documentation
--------
This documentation focuses on the technical and development aspects of the dataset, e.g. how we produce the dataset.
To this end we present and describe our working environment, our data pipeline and our data
pipeline management tool :code:`cowid`. 

Our goal is that this documentation helps users and organizations that want to contribute to the project.


.. seealso::

   - If you want to explore all our charts and publications, visit `our COVID-19 pandemic website section <https://ourworldindata.org/coronavirus>`_
   - If you want to explore the source code, visit `the GitHub project repository
     <https://github.com/owid/covid-19-data>`_



Contents
-------

.. toctree::
   :maxdepth: 1

   Dataset <dataset>
   Working environment <source/work-env>
   Data pipeline <source/data-pipeline>
   cowid API <source/cowid-api>
   Contribute <source/contribute>
   cowidev <source/cowidev>



cowidev
-------
**cowidev** is a python package developed at `Our World in Data <https://ourworldindata.org>`_ to maintain and produce
`our global COVID-19 dataset <https://ourworldindata.org/coronavirus>`_. It contains several tools for all the COVID
data pipelines, including vaccinations and testing. Due to the nature of the problem, this library is in
development and no stable version is expected anytime soon. You can explore all the source code on our `GitHub
repository <https://github.com/owid/covid-19-data>`_.
