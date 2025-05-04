# PumpFun Auto Sniper Setup Guide

## Overview

This guide helps you set up and run the PumpFun auto sniper bot from the Solana Trading CLI. The bot uses gRPC to stream transaction data from Solana and automatically snipes new tokens created on Pump.fun.

## Prerequisites

1. Node.js and npm installed
2. Solana wallet with funds
3. RPC endpoint (like Helius)
4. gRPC token (from providers like Shyft)

## Installation Steps

1. Clone the repository
```bash
git clone https://github.com/jacobbyrne72/solana-trading-cli.git
cd solana-trading-cli
```

2. Install dependencies
```bash
npm install
```

3. Configure the environment variables:
   - Copy the template file from `src/helpers/.env.example` to `src/helpers/.env`
   - Edit the `.env` file with your information:
     - Add your Solana wallet private key
     - Add your RPC endpoint (Helius recommended)
     - Set your gRPC token and URL
     - Configure sniper settings (quote amount, auto-sell, etc.)

Example .env configuration:
```
# Mainnet configuration
PRIVATE_KEY = YOUR_PRIVATE_KEY_HERE

# RPC Endpoints
MAINNET_ENDPOINT = "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_API_KEY"
WS_MAINNET_ENDPOINT = "wss://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_API_KEY"

# Jito configuration (optional, for faster transactions)
JITO_FEE = "0.0001"
BLOCK_ENGINE_URL=tokyo.mainnet.block-engine.jito.wtf

# gRPC configuration
COMMITMENT_LEVEL=confirmed
GRPC_XTOKEN="YOUR_GRPC_TOKEN"
GRPC_URL="https://grpc.fra.shyft.to"
LOG_LEVEL=info

# PumpFun sniper bot settings
QUOTE_AMOUNT=0.05 # Amount in SOL to use for each buy
AUTO_SELL=true # Whether to auto-sell tokens after buying
AUTO_SELL_TIMEOUT=5 # Time in seconds to wait before selling
```

## Running the Bot

### Snipe Any New Tokens

To snipe any new tokens created on Pump.fun:

```bash
ts-node src/grpc_streaming_dev/grpc-pf-sniper/src/streaming/snipe-create.ts --auto-sell --jito --n 3
```

This command:
- `--auto-sell`: Automatically sells tokens after buying
- `--jito`: Uses Jito for faster transaction processing (optional)
- `--n 3`: Limits the bot to snipe only 3 tokens

### Snipe a Specific Token

To snipe a specific token when it's created:

```bash
ts-node src/grpc_streaming_dev/grpc-pf-sniper/src/streaming/snipe-create.ts --token TOKEN_ADDRESS --auto-sell --jito
```

Replace `TOKEN_ADDRESS` with the token mint address you want to snipe.

## Command Options

- `--token <TOKEN_ADDRESS>`: Specify a token address to snipe
- `--auto-sell`: Enable automatic selling
- `--sell-after <NUMBER>`: Sell after a specific number of buys
- `--n <NUMBER>`: Set the number of tokens to snipe
- `--jito`: Enable Jito for faster transactions
- `-h, --help`: Display help information

## Notes

- The bot streams data from Pump.fun's mint authority to detect new tokens
- Auto-sell will liquidate your position after the time specified in `AUTO_SELL_TIMEOUT`
- Using Jito requires having the proper Jito configuration in your .env file

## Security Warning

- Never share your `.env` file or private key
- Use this bot at your own risk
- This is for educational purposes only