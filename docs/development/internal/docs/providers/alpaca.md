# Alpaca Data Provider

## Overview

The Alpaca Data Provider integrates with the Alpaca Markets API to fetch historical and real-time market data. It supports both free (IEX) and paid (SIP) data feeds.

## Configuration

### API Keys

Set your Alpaca API credentials as environment variables:

```bash
export ALPACA_KEY="your_api_key"
export ALPACA_SECRET="your_api_secret"
```

### Feed Types

- **IEX Feed** (Free): Limited to IEX data, suitable for development and testing
- **SIP Feed** (Paid): Full market data with broader coverage

## Data Format

### Timestamps

The Alpaca API returns timestamps in ISO 8601 format (e.g., "2024-01-15T09:30:00Z"). The `t` field in Alpaca bars is **milliseconds since epoch** when returned as a numeric value, but is typically provided as an ISO string.

**Important**: The MarketPipe Alpaca adapter correctly handles timestamp conversion from ISO strings to nanoseconds for internal storage and processing.

### Bar Structure

Alpaca returns OHLCV bars with the following structure:

```json
{
  "t": "2024-01-15T09:30:00Z",  // Timestamp (ISO string)
  "o": 150.25,                  // Open price
  "h": 151.00,                  // High price  
  "l": 149.50,                  // Low price
  "c": 150.75,                  // Close price
  "v": 125000                   // Volume
}
```

## Rate Limits

- **IEX Feed**: 200 requests per minute
- **SIP Feed**: Higher limits based on subscription

## Usage Examples

### CLI Ingestion

```bash
# Using IEX feed (free)
marketpipe ohlcv ingest \
  --provider alpaca \
  --feed-type iex \
  --symbols AAPL,MSFT \
  --start 2024-01-01 \
  --end 2024-01-31 \
  --output data/stocks

# Using SIP feed (paid)
marketpipe ohlcv ingest \
  --provider alpaca \
  --feed-type sip \
  --symbols SPY \
  --start 2024-01-01 \
  --end 2024-12-31 \
  --output data/etfs
```

### Configuration File

```yaml
provider: alpaca
feed_type: iex
symbols:
  - AAPL
  - GOOGL
  - MSFT
start: 2024-01-01
end: 2024-12-31
output_path: data/stocks
batch_size: 500
workers: 3
```

## Troubleshooting

### Common Issues

1. **Invalid API Keys**: Ensure your `ALPACA_KEY` and `ALPACA_SECRET` environment variables are set correctly
2. **Rate Limiting**: Reduce the number of workers or add delays between requests
3. **Weekend/Holiday Data**: Alpaca may not return data for non-trading days
4. **Historical Data Limits**: IEX feed has limited historical data availability

### Error Messages

- `401 Unauthorized`: Check your API credentials
- `429 Too Many Requests`: You've hit the rate limit, wait and retry
- `403 Forbidden`: Your subscription may not include the requested data feed 