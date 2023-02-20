#!/bin/bash

CURR_DIR=$(basename "$(pwd)")

if [ "$CURR_DIR" != "totes-data-project" ]; then
    echo -e "To run this script you need to be in the totes-data-project directory"
else
    cd ./src/packages
    zip -r ../ingest.zip .
    cd ..
    zip ingest.zip query_totesys_db.py
    cd ..
    cd function/package
    zip -r ../lambdalayer.zip .
    cd ..
    zip lambdalayer.zip app.py
    cd ..
    cd terraform
    terraform plan
fi