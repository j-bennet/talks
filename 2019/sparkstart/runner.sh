#!/usr/bin/env bash

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 8g \
    --executor-memory 3g \
    --num-executors 4 \
    --executor-cores 4 \
    --conf "spark.yarn.executor.memoryOverhead=2g" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/hadoop/log4j.properties" \
    --py-files my_custom_package.egg \
    --jars my_extra_java_lib.jar \
    driver.py
