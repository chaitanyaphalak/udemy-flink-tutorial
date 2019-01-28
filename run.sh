#!/bin/bash

argArray=("$@")
SCRIPT_DIRECTORY=$(cd `dirname $0` && pwd)
PROJECT_HOME="$SCRIPT_DIRECTORY/${argArray[0]}"
if [[ ! -d "$PROJECT_HOME" ]]
then
  echo "Given project home does not exist. Please provide another home directory for the project"
  exit
fi

cd "$PROJECT_HOME"
mvn clean package

JAR_FILE=`find . -name \*.jar`
if [[ -z "$JAR_FILE" ]]
then
  echo "Project jar was not found after building"
  exit
fi

echo "JAR found: $JAR_FILE"

VAR_FILE="$PROJECT_HOME/variables.txt"
JOB_ARGS=""
echo "VAR file: $VAR_FILE"
if [[ -f "$VAR_FILE" ]] 
then
  echo "Variables file found. Parsing job arguments"
  while IFS=, read -ra arr; do
    JOB_ARGS="$JOB_ARGS --${arr[0]} ${arr[1]}"
  done < "$VAR_FILE"
  echo "Job args parsed: $JOB_ARGS"
else
  echo "No variables file found. Executing job with no parameters"
fi

flink run "$JAR_FILE" $JOB_ARGS 

