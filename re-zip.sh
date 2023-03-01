#!/bin/bash

cd src
zip ingest_deployment ingest_data.py
zip transform_deployment transform_data.py
zip load_deployment load_data.py
cd ..