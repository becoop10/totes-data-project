#!/bin/bash

echo "Would lambda would you like to zip [i]ngest, [t]ransform, [l]oad, [a]ll?"
read input

if [ $input != "i" ] && [ $input != "t" ] && [$input != "l" ] && [ $input != "a" ]
then
    echo 'Invalid $input'
fi

if [ $input = 'i' ] || [ $input = 'a' ]
then
    rm src/ingest_deployment.zip
    pip install psycopg2-binary --target src/ingest_package
    pip install datetime --target src/ingest_package
    cd src/ingest_package
    zip -r ../ingest_deployment.zip .
    cd ..
    zip ingest_deployment.zip ingest_data.py
    #rm -r ingest_package
    cd ..
fi

if [ $input = 't' ] || [ $input = 'a' ]
then
    rm src/transform_deployment.zip
    pip install pyarrow --target src/transform_package
    cp src/utils/myutils.py src/transform_package
    cd src/transform_package
    rm -r numpy numpy.libs numpy-1.24.2.dist-info
    zip -r ../transform_deployment.zip .
    cd ..
    zip transform_deployment.zip transform_data.py
    #rm -r transform_package
    cd ..
fi

if [ $input = 'l' ] || [ $input = 'a' ]
then
    rm src/load_deployment.zip
    pip install psycopg2-binary --target src/load_package
    pip install pyarrow --target src/load_package
    cd src/load_package
    rm -r numpy numpy.libs numpy-1.24.2.dist-info
    zip -r ../load_deployment.zip .
    cd ..
    #rm -r load_package
    zip load_deployment.zip load_data.py
    cd ..
fi
