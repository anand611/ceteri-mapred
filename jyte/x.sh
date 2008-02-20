#!/bin/bash

## change these two lines
VERSION=0.15.3
HADOOP=~/hadoop-${VERSION}

## build the JAR file

javac -classpath ${HADOOP}/hadoop-${VERSION}-core.jar -d classes src/JyteRank.java 
jar -cvf jyterank.jar -C classes/ .

## setup the HDFS directory structure

rm -rf input from2to prevrank elemrank thisrank
${HADOOP}/bin/hadoop fs -put cred.txt input/cred01

## run the jobs

${HADOOP}/bin/hadoop jar jyterank.jar JyteRank input from2to prevrank elemrank thisrank

${HADOOP}/bin/hadoop fs -ls prevrank
#${HADOOP}/bin/hadoop fs -cat prevrank/part-00000
