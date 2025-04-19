#!/bin/zsh
openapi-generator generate \
  -i ./generated/isone/swagger_isone_responses_generic.yaml \
  -g python \
  -o ./generated/isone/generated_code \
  --global-property apiDocs=false,modelDocs=false \
  --package-name isone_client

SETUP_FILE=./generated/isone/generated_code/setup.py

# Insert package_dir line above packages=
sed -i '' '/packages=/i\
    package_dir={"": "."},
' "$SETUP_FILE"

# Replace packages= line
sed -i '' 's/packages=find_packages([^)]*)/packages=find_packages(".", exclude=["test", "tests"])/' "$SETUP_FILE"
