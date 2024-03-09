#!/bin/bash

API_KEY='$2a$10$N8qC0t8MkXA1lYV1QlqVju/G3UvLX8nK7flamqx2edin5TulU8FP6'
COLLECTION_ID='659a4dd9dc746540188e13da'

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @dogs.json \
    "https://api.jsonbin.io/v3/b"