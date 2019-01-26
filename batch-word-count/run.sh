#!/bin/bash

ROOT_PATH="/home/scott/projects/udemy-flink-tutorial/batch-word-count"
JAR_PATH="$ROOT_PATH/target/test-flink-1.0-SNAPSHOT.jar"
INPUT_FILE="$ROOT_PATH/wc.txt"
OUTPUT_FILE="$ROOT_PATH/result"

if [ ! -f "$JAR_PATH" ]
then
  echo "Jar file not found. Please build and try again."
  exit
fi

if [ ! -f "$INPUT_FILE" ]
then
  echo "Input file not found at $INPUT_FILE"
  exit 
fi

if [ -f "$OUTPUT_FILE" ]
then
  echo "Removing existing output file found at $OUTPUT_FILE"
  rm $OUTPUT_FILE
fi

flink run $JAR_PATH --input $INPUT_FILE --output $OUTPUT_FILE