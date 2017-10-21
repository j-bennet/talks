#!/usr/bin/env bash

# Data from http://ourairports.com/data/

wget -r -A "*.csv" -I "data" -nH -e robots=off http://ourairports.com/
