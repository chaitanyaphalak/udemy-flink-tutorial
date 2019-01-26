#!/bin/bash

ROOT="/home/scott/projects/udemy-flink-tutorial/word-count-streaming"
JAR_PATH="$ROOT/target/word-count-streaming-1.0-SNAPSHOT.jar"

cd "$ROOT"
mvn clean package

if [ ! -f "$JAR_PATH" ]
then
    echo "Jar file not found at '$JAR_PATH'. Please recompile"
    exit
fi

flink run "$JAR_PATH"