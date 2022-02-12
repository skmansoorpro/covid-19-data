# Data pipeline
To produce [our dataset](../dataset) we are constantly developing our dedicated library [cowidev](../cowidev/index). This library provides us with the
command tool [`cowid`](../cowidev/cowid-api) which eases:

1. Running several _sub-processes_ (or pipelines) that generate _intermediate datasets_.
2. Jointly processing and merging all these intermediate datasets into the final and complete dataset.  

Consequently, the dataset is updated multiple times a day (_at least_ at 06:00 and 18:00 UTC), using the latest generated intermediate datasets.


## Dataset pipelines
The dataset pipeline is built from several pipelines, which are executed independently and whose outputs are combined in
a final step. The complexity of the pipelines varies. For instance, for vaccinations, testing and hospitalization
we are responsible for collecting, processing and publishing the data but for cases/deaths we leave the collection step to [Johns
Hopkins Coronavirus Resource Center](https://coronavirus.jhu.edu/map.html) and then transform and publish the data.

The table below lists all the constituent pipelines, along with their execution frequencies, and what are the pipelines'
tasks.

| **Pipeline**              | **Frequency**                | **Tasks**                             |
|---------------------------|------------------------------|------------------------------------------|
| [Vaccinations](#vaccinations)               | daily at 12:00 UTC           | {abbr}`Collection (Scraping primary sources (e.g. country governmental sites) and extracting relevant datapoints.)`, {abbr}`transformation (Transforming and cleaning the downloaded data into a human-readable format.)`, {abbr}`presentation (Presenting the cleaned data to the public (e.g. charts, dataset files, etc.).)` |
| [Testing](#testing)                   | 3 times per week             | Collection, transformation, presentation |
| [Hospitalization & ICU](#hospitalization-icu)     | daily at 06:00 and 18:00 UTC | Collection, transformation, presentation |
| [Cases & Deaths (JHU)](#cases-deaths-jhu)      | every hour (if new data)     | Transformation, presentation             |
| [Excess mortality](#excess-mortality)          | daily at 06:00 and 18:00 UTC | Transformation, presentation             |
| [Variants](#variants)                  | daily at 20:00 UTC           | Transformation, presentation             |
| [Reproduction rate](#reproduction-rate)         | daily                        | Presentation                             |
| [Policy responses (OxCGRT)](#policy-responses-oxcgrt) | daily                        | Transformation, presentation             |

You can find all the automation details [in this file](https://github.com/owid/covid-19-data/blob/master/scripts/scripts/autoupdate.sh)

### Vaccinations
The vaccination pipeline is probably the most complete one, where we scrape and extract data for each country in the
dataset.

The pipeline is executed manually, by [@edomt](https://github.com/edomt) or [@lucasrodes](https://github.com/lucasrodes) on
a daily basis before 12 UTC.

#### Execution steps
```
# Download/scrape data
cowid vax get

# Proces/check data
cowid vax process

# Generate dataset
cowid vax generate

# Integrate into full dataset
cowid vax export
```

```{seealso}

[Intermediate datasets](https://github.com/owid/covid-19-data/owid/blob/master/public/data/vaccinations/), including per-country files.
```

### Testing
The pipeline is executed manually, by [@camapel](https://github.com/camapel) on Mondays and Fridays.

:::{warning}
The testing pipeline is [under refactoring](https://github.com/owid/covid-19-data/discussions/2099).
:::

#### Execution steps

```
# Download/scrape data
cowid testing get
```


### Hospitalization & ICU

#### Execution steps

```
# Download data & generate dataset
cowid hosp generate

# Update Grapher-ready files
cowid hosp grapher-io

# Update Grapher database
cowid hosp grapher-db
```


### Cases & Deaths (JHU)

#### Execution steps

```
# Download data
cowid jhu download

# Generate dataset
cowid jhu generate

# Update Grapher database
cowid jhu grapher-db
```

### Excess Mortality

#### Execution steps

```
# Download data and generate dataset
cowid xm generate
```

### Variants

#### Execution steps

```
# Download data and generate dataset
cowid variants generate

# Update Grapher-ready files
cowid variants grapher-io
```

### Reproduction rate
TODO

### Policy responses (OxCGRT)

:::{warning}
The OxCGRT pipeline is under construction.
:::

