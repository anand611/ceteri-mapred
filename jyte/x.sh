#!/bin/bash

# run Hadoop iterations to approximate jyte cred points
#
# Paco NATHAN http://code.google.com/p/ceteri-mapred/

## change the following two lines as needed
VERSION=0.15.3
HADOOP=~/hadoop-${VERSION}

## build the JAR file

javac -classpath ${HADOOP}/hadoop-${VERSION}-core.jar -d classes src/org/ceteri/JyteRank.java 
jar -cvf jyterank.jar -C classes/ .

## setup the HDFS directory structure

rm -rf input from2to prevrank elemrank thisrank
${HADOOP}/bin/hadoop fs -put cred.txt input/cred01

## run the jobs

${HADOOP}/bin/hadoop jar jyterank.jar org.ceteri.JyteRank input from2to prevrank elemrank thisrank

## uncomment the following two line to compare N results
#N=10
#${HADOOP}/bin/hadoop fs -cat prevrank/part-00000 | head -${N} | ./crawl.pl > compare.tsv
