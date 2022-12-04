#!/bin/bash

curl \
  --data-urlencode "update=INSERT { ?s ?p ?o } WHERE { ?s ?p ?o }" \
  --data-urlencode "track-result-size=true" \
  $ENDPOINT > ${1}
