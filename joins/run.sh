#!/bin/bash

ROOT_PATH="/home/scott/projects/udemy-flink-tutorial/joins"
JAR_PATH="$ROOT_PATH/target/joins-1.0-SNAPSHOT.jar"
INPUT_FILE1="$ROOT_PATH/person.txt"
INPUT_FILE2="$ROOT_PATH/location.txt"
OUTPUT_FILE="$ROOT_PATH/result"

if [ ! -f "$JAR_PATH" ]
then
  echo "Jar file not found. Please build and try again."
  exit
fi

if [ ! -f "$INPUT_FILE1" ]
then
  echo "Input file not found at $INPUT_FILE1"
  exit 
fi

if [ ! -f "$INPUT_FILE2" ]
then
  echo "Input file not found at $INPUT_FILE2"
  exit 
fi

if [ -f "$OUTPUT_FILE" ]
then
  echo "Removing existing output file found at $OUTPUT_FILE"
  rm $OUTPUT_FILE
fi

flink run $JAR_PATH --input1 $INPUT_FILE1 --input2 $INPUT_FILE2 --output $OUTPUT_FILE