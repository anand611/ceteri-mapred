#!/bin/bash

# crawl jyte.com profiles to extract cred point values for specific
# OpenID URIs
#
# Paco NATHAN http://code.google.com/p/ceteri-mapred/

TMPFILE=`mktemp wgetXXXXX`
wget -o /dev/null -O ${TMPFILE} http://jyte.com/profile/$1
grep -B 2 ".h3.Cred Points..h3." ${TMPFILE} | head -1 | perl -pe 's/^\s+\<h1\>([\d\.]+)\<.*$/$1/g;'
rm ${TMPFILE}