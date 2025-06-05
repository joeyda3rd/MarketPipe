# Connector Notes

## Alpaca

* Endpoint: `GET https://data.sandbox.alpaca.markets/v2/stocks/{symbol}/bars`
* Auth: headers `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY`
* Credentials should be provided via environment variables `ALPACA_KEY` and `ALPACA_SECRET`
* Sandbox rate limit: 200 requests/min (burst 100)
* Pagination token field: `next_page_token` (supply as `page_token`)
