#!/usr/bin/env bash

cd packages/
#pwd
rm -rf ../packages.zip
zip -r ../packages.zip .
cd ..
#pwd
spark-submit --master yarn\
 --py-files packages.zip test.py

# --jars spark-streaming-kafka-0-10-assembly_2.12-3.1.2.jar /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.1.2.jar
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2