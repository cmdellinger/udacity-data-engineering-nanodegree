#!/bin/bash

/opt/airflow/start.sh

# start airflow scheduler in background
# `airflow scheduler` keeps not starting
# nohup airflow scheduler &>/dev/null &