#!/bin/bash

export SPARK_APPLICATION_ARGS="${APP_ARGS_OWNER} ${APP_ARGS_INPUT} ${APP_ARGS_OUTPUT}"

sh /submit.sh
