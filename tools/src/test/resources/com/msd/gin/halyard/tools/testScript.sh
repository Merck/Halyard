#!/bin/bash

curl \
  --data-urlencode "query=SELECT * WHERE { ?s ?p ?o . } LIMIT 10" \
  $ENDPOINT

