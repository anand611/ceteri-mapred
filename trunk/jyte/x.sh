#!/bin/bash

## change the following two lines as needed
VERSION=0.15.3
HADOOP=~/hadoop-${VERSION}

## compare N results

N=10
${HADOOP}/bin/hadoop fs -cat prevrank/part-00000 | head -${N} | ./crawl.pl > compare.tsv
