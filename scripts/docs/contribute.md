# Contribute
Hi! Thank you so much for wanting to contribute :).

This is a work-in-progress document, so if you have questions you can [submit and
issue](https://github.com/owid/covid-19-data/issues/new). Also, bear in mind that all the development activity, including
open discussions occur on Github, so make sure to check the latest [issues](https://github.com/owid/covid-19-data/issues) and [dicussions](https://github.com/owid/covid-19-data/discussions). 
## Pre requisites
First thing that you need to do is [forking the
repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo) [owid/covid-19-data](https://github.com/owid/covid-19-data). Next, you need to install our library and
all dependencies.
For the latter, make sure to follow our [guideline](environment). Without a proper working environment you will not be
able to run the scripts and test your changes.

## Contributing
Our pipeline is complex and is built from several processes. Feel free to familiarize yourself with these [in our
pipeline overview](data-pipeline).

Most of the contributions that we receive are in the context of vaccinations, testing and hospitalizations. What is
different in these pipelines compared to others is that we are responsible for scraping the data individually for
each country. Therefore, we need to maintain a large amount of individual country scripts which can stop working from
night to day.

We have created domain-specific guidelines for these, so it is easier for you to contribute!

- [Contribute to vaccinations](contribute-vax)
- [Contribute to testing](contribute-test)