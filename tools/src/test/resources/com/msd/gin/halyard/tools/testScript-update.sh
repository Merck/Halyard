#!/bin/bash

curl \
  --data-urlencode "update=INSERT DATA { <http://whatever/newSubj> <http://whatever/newPred> <http://whatever/newObj>. }" \
  $ENDPOINT

curl \
  -H "Accept: text/boolean" \
  --data-urlencode "query=ASK { <http://whatever/newSubj> <http://whatever/newPred> <http://whatever/newObj>. }" \
  $ENDPOINT > ${1}