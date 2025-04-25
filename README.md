# Energy Price Forecasting Model


## Work in Progress (Not functional yet!)

### Generating the EIA client

Run the following to generate the EIA python client bindings from the EIA swagger.

I used chatgpt to convert their swagger doc to a valid openapi client generation source
(The raw source throws errors/warnings - I don't think it was originally meant for client code generation weirdly)

```bash
./generate_eia_client.sh
```

Raw Swagger is found here: https://www.eia.gov/opendata/eia-api-swagger.zip


EIA API Key must be set like so:
```
# .env file in repo root
EIA_API_KEY=your_key_here
```