#!/bin/bash

while [ true ]
do
    echo `date`
    #echo `date` >> sync.log
    #python syncSQLErrors.py >> sync.log
    python syncSQLErrors.py
    sleep 2m
done
