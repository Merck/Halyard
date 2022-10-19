#!/bin/bash

curl ${ENDPOINT}query1 > ${1}
curl ${ENDPOINT}query2 >> ${1}
