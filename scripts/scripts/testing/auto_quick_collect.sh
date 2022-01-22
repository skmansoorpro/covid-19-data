#!/bin/bash
set -e
cd ~/git/covid-19-data/scripts/
source venv/bin/activate
cd ~/git/covid-19-data/scripts/scripts/testing
bash collect_data.sh quick
git add automated_sheets/*
git commit -m "data(testing): automated collection"
git push
