#!/bin/zsh
openapi-generator generate \
  -i ./generated/eia/eia-api-swagger-fixed.yaml \
  -g python \
  -o ./generated/eia/generated_code \
  --package-name eia_client

SETUP_FILE=./generated/eia/generated_code/setup.py

# Insert package_dir line above packages=
sed -i '' '/packages=/i\
    package_dir={"": "."},
' "$SETUP_FILE"

# Replace packages= line
sed -i '' 's/packages=find_packages([^)]*)/packages=find_packages(".", exclude=["test", "tests"])/' "$SETUP_FILE"
