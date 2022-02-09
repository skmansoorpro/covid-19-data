# How do we update our dataset?

We share the complete dataset as [CSV](https://covid.ourworldindata.org/data/owid-covid-data.csv),
[JSON](https://covid.ourworldindata.org/data/owid-covid-data.json)
and [XLSX](https://covid.ourworldindata.org/data/owid-covid-data.xlsx) files. This dataset contains many metrics. More details about the dataset can be found [here](https://github.com/owid/covid-19-data/tree/master/public/data).

We produce this dataset by

1. Running several _sub-processes_ that generate intermediate datasets.
2. Jointly processing and merging all these intermediate datasets into the final and complete dataset.  

Consequently, the dataset is updated multiple times a day (_at least_ at 06:00 and 18:00 UTC), using the latest generated intermediate datasets.


## Dataset sub-processes

Find below a diagram with the different sub-processes, their approximate update frequency and intermediate generated
datasets. This diagram only shows the sub-processes relevant for the production of the complete dataset, as there are
other sub-processes producing data that may appear on our website (Grapher) but that is not present in the complete dataset.

| **Pipeline**              | **Frequency**                | **Module**       | **Steps** | **Output**                                                                      |
|---------------------------|------------------------------|------------------|-----------|---------------------------------------------------------------------------------|
| Cases & Deaths (JHU)      | every hour (if new data)     | cowidev.jhu      |           | jhu/                                                                            |
| Vaccination               | daily at 12:00 UTC           | cowidev.vax      |           | vaccinations.csv,vaccinations-by-manufacturer.csv,vaccinations-by-age-group.csv |
| Hospitalization & ICU     | daily at 06:00 and 18:00 UTC | cowidev.hosp     |           | covid-hospitalizations.csv                                                      |
| Testing                   | 3 times per week             | cowidev.testing  |           | covid-testing-all-observations.csv                                              |
| Policy responses (OxCGRT) | daily                        | cowidev.oxcgrt   |           | latest.csv                                                                      |
| Variants                  | daily at 20:00 UTC           | cowidev.variants |           | covid-variants.csv, covid-sequencing.csv                                        |
| Excess mortality          | daily at 06:00 and 18:00 UTC | cowidev.xm       |           | excess_mortality.csv, excess_mortality_economist_estimates.csv                  |
| Reproduction rate         | daily                        |                  |           |                                                                                 |

<pre>
  ┌──────────────────────────────────────────────────────────┐
  │ Cases & Deaths (JHU)                                     │
  │                                                          │
  │  module: <a href="../../scripts/src/cowidev/jhu/__main__.py">cowidev.jhu</a>                                     │
  │  update: every hour (if new data)                        │
  │                                                          │
  │           ┌───┐    ┌────────────┐                        │
  │  steps:   │<a href="../../scripts/src/cowidev/jhu/__main__.py">etl</a>├───►│<a href="../../scripts/src/cowidev/jhu/__main__.py">grapher-file</a>│                        │
  │           └───┘    └────────────┘                        │
  │                                                          │
  │                                                          │
  │  output:  <a href="jhu/">jhu/</a>─────────────────────────────────────────── ──────────┐
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="vaccination/">Vaccination</a>                                              │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/vax/">cowidev.vax</a>                                     │          │
  │  update: daily at 12:00 UTC                              │          │
  │                                                          │          │
  │           ┌───┐     ┌───────┐     ┌────────┐    ┌──────┐ │          │
  │  steps:   │<a href="../../scripts/src/cowidev/vax/cmd/get_data.py">get</a>├────►│<a href="../../scripts/src/cowidev/vax/cmd/process_data.py">process</a>├────►│<a href="../../scripts/src/cowidev/vax/cmd/generate_dataset.py">generate</a>├───►│<a href="../../scripts/src/cowidev/vax/cmd/export.py">export</a>│ │          │
  │           └───┘     └───────┘     └────────┘    └──────┘ │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="vaccinations/vaccinations.csv">vaccinations.csv</a> ────────────────────────────── ──────────│
  │           <a href="vaccinations/vaccinations-by-manufacturer.csv">vaccinations-by-manufacturer.csv</a>               │          │
  │           <a href="vaccinations/vaccinations-by-age-group.csv">vaccinations-by-age-group.csv</a>                  │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="hospitalizations/">Hospitalization & ICU</a>                                    │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/hosp/__main__.py">cowidev.hosp</a>                                    │          │
  │  update: daily at 06:00 and 18:00 UTC                    │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌──────────┐      │          │
  │  steps:   │<a href="../../scripts/src/cowidev/hosp/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/hosp/grapher.py">grapher-file</a>├────►│<a href="../../scripts/src/cowidev/hosp/grapher.py">grapher-db</a>│      │          │
  │           └───┘     └────────────┘     └──────────┘      │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="hospitalizations/covid-hospitalizations.csv">covid-hospitalizations.csv</a> ──────────────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │      
  ┌──────────────────────────────────────────────────────────┐          │      
  │ Testing                                                  │          │                                      
  │                                                          │          │      
  │  module: <a href="../../scripts/src/cowidev/testing">cowidev.testing</a>                                 │          |
  │  update: 3 times per week                                │          │                                     
  │                                                          │          │      
  │           ┌───┐     ┌────────────────┐                   │          │      
  │  steps:   │<a href="../../scripts/src/cowidev/cmd/testing/get/">get</a>├────►│<a href="../../scripts/scripts/testing/generate_dataset.R">generate_dataset</a>│                   │          │      
  │           └───┘     └────────────────┘                   │          │
  │                                                          │          │
  │  output:  <a href="testing/covid-testing-all-observations.csv">covid-testing-all-observations.csv</a> ──────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Policy responses (OxCGRT)                                │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/oxcgrt/__main__.py">cowidev.oxcgrt</a>                                  │          │
  │  update: daily                                           │          │
  │                                                          │          │
  │           ┌───┐    ┌────────────┐    ┌──────────┐        │          │
  │  steps:   │<a href="../../scripts/src/cowidev/oxcgrt/etl.py">etl</a>├───►│<a href="../../scripts/src/cowidev/oxcgrt/grapher.py">grapher-file</a>├───►│<a href="../../scripts/src/cowidev/oxcgrt/grapher.py">grapher-db</a>│        │          │
  │           └───┘    └────────────┘    └──────────┘        │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="../../scripts/input/bsg/latest.csv">latest.csv</a> ──────────────────────────────────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="variants/">Variants</a>                                                 │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/variants/__main__.py">cowidev.variants</a>                                │          │
  │  update: daily at 20:00 UTC                              │          │
  │                                                          │          │
  │           ┌───┐     ┌────────────┐     ┌─────────────┐   │          │
  │  steps:   │<a href="../../scripts/src/cowidev/variants/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/variants/grapher.py">grapher-file</a>├────►│<a href="../../scripts/src/cowidev/variants/grapher.py">explorer-file</a>│   │          │
  │           └───┘     └────────────┘     └─────────────┘   │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  covid-variants.csv ──────────────────────────── ──────────┤
  │           covid-sequencing.csv                           │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ <a href="excess_mortality/">Excess mortality</a>                                         │          │
  │                                                          │          │
  │  module: <a href="../../scripts/src/cowidev/xm/__main__.py">cowidev.xm</a>                                      │          │
  │  update: daily at 06:00 and 18:00 UTC                    │          │
  │                                                          │          │
  │           ┌───┐                                          │          │
  │  steps:   │<a href="../../scripts/src/cowidev/xm/etl.py">etl</a>│                                          │          │
  │           └───┘                                          │          │
  │                                                          │          │
  │                                                          │          │
  │  output:  <a href="excess_mortality/excess_mortality.csv">excess_mortality.csv</a> ────────────────────────── ──────────┤
  │           <a href="excess_mortality/excess_mortality_economist_estimates.csv">excess_mortality_economist_estimates.csv</a> ────── ──────────┤
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
  ┌──────────────────────────────────────────────────────────┐          │
  │ Reproduction rate                                        │          │
  │                                                          │          │
  │  <a href="https://github.com/crondonm/TrackingR/blob/main/Estimates-Database/database_7.csv">remote file</a> ──────────────────────────────────────────── ──────────┘     
  │                                                          │          │
  └──────────────────────────────────────────────────────────┘          │
                                                                        │
    ┌──────────────────────────────────┐                                │
    │ Megafile                         │                                │
    │                                  │                                │
    │  module: cowidev.megafile        │                                │
    │  update: daily at 6h and 18h UTC │◄───────────────────────────────┘
    │                                  │
    │  output:  <a href="https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv">owid-covid-data.csv</a>    │
    │                                  │
    └──────────────────────────────────┘
</pre>


## Other subprocesses

The following sub-processes generate other intermediate datasets relevant for our Grapher and Explorer charts (their
metrics are not present in the compelete dataset).

<pre>
  ┌──────────────────────────────────────────────────────────┐
  │ <a href="vaccination/">Vaccination US States</a>                                    │
  │                                                          │
  │  module: <a href="../../scripts/src/cowidev/vax/us_states/__main__.py">cowidev.vax.us_states</a>                           │
  │  update: every hour                                      │
  │                                                          │
  │           ┌───┐     ┌────────────┐                       │
  │  steps:   │<a href="../../scripts/src/cowidev/vax/us_states/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/vax/us_states/grapher.py">grapher-file</a>│                       │
  │           └───┘     └────────────┘                       │
  │                                                          │
  │                                                          │
  │  output:  <a href="vaccinations/us_state_vaccinations.csv">us_state_vaccinations.csv</a>                      │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ <a href="vaccination/">Local UK Data</a>                                            │
  │                                                          │
  │  module: <a href="../../scripts/scripts/uk_nations.py">uk_nations.py</a>                                   │
  │  update: daily at 17:00 UTC                              │
  │                                                          │
  │           ┌────────────────┐    ┌─────────┐              │
  │  steps:   │<a href="../../scripts/scripts/uk_nations.py">generate_dataset</a>├───►│<a href="../../scripts/scripts/uk_nations.py">update_db</a>│              │
  │           └────────────────┘    └─────────┘              │
  │                                                          │
  │                                                          │
  │  output:  <a href="../../scripts/grapher/uk_covid_data.csv">uk_covid_data.csv</a>                              │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ Google Mobility                                          │
  │                                                          │
  │  module: <a href="../../scripts/src/cowidev/gmobility/__main__.py">cowidev.gmobility</a>                               │
  │  update: daily at 15:00 UTC                              │
  │                                                          │
  │           ┌───┐     ┌────────────┐                       │
  │  steps:   │<a href="../../scripts/src/cowidev/gmobility/etl.py">etl</a>├────►│<a href="../../scripts/src/cowidev/gmobility/grapher.py">grapher-file</a>│                       │
  │           └───┘     └────────────┘                       │
  │                                                          │
  │                                                          │
  │  output:  <a href="../../scripts/grapher/Google Mobility Trends (2020).csv">Google Mobility Trends (2020).csv</a>              │
  │                                                          │
  └──────────────────────────────────────────────────────────┘

</pre>
