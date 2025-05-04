# Solana gRPC Streaming Module

This module provides a robust real-time streaming solution for Solana blockchain data using gRPC. It enables high-performance monitoring of transactions, accounts, and slots with minimal latency.

## Features

- **Real-time Transaction Monitoring**: Stream transactions as they happen on the blockchain
- **Account State Tracking**: Monitor changes to specific accounts
- **Slot Updates**: Follow new slots being produced on the blockchain
- **Auto-Reconnection**: Automatically reconnect if the stream is disrupted
- **Buffer Conversion**: Utilities to convert binary buffer data to readable formats
- **Custom Subscriptions**: Create tailored subscriptions for specific use cases

## Components

### GrpcStreamingClient

The main class that manages gRPC connections and streaming:

```typescript
const client = new GrpcStreamingClient(
  "mainnet.helius-rpc.com:443", 
  "YOUR_API_KEY", 
  true  // Auto-reconnect on errors
);
```

### Example Usage

#### Monitoring a Program's Transactions

```typescript
// Subscribe to a program's transactions
await client.subscribeToProgram(
  "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  // Pump.fun program ID
  (data) => {
    // Handle transaction data
    console.log("New transaction:", data);
  },
  (error) => {
    // Handle errors
    console.error("Stream error:", error);
  }
);
```

#### Tracking Specific Accounts

```typescript
// Track updates to specific accounts
await client.subscribeToAccounts(
  ["AccountAddress1", "AccountAddress2"],
  (data) => {
    // Handle account update
    console.log("Account updated:", data);
  }
);
```

#### Custom Subscriptions

```typescript
// Create a custom subscription
const request = {
  accounts: {},
  slots: {},
  transactions: {
    custom: {
      vote: false,
      failed: false,
      accountInclude: ["AccountOrProgramToMonitor"],
    },
  },
  commitment: CommitmentLevel.PROCESSED,
  // Additional fields as needed
};

await client.customSubscribe(
  request,
  (data) => {
    // Handle data
    console.log("Received data:", data);
  }
);
```

## Examples

This module includes ready-to-use examples:

1. **`stream-pumpfun.ts`**: Monitor pump.fun token transactions
2. **`token-sniper.ts`**: Advanced bot for sniping new tokens

## Environment Configuration

Add these to your `.env` file:

```
# gRPC Settings
YELLOWSTONE_GRPC_URL=mainnet.helius-rpc.com:443
HELIUS_API_KEY=YOUR_API_KEY
YELLOWSTONE_PING_INTERVAL=30000
GRPC_RECONNECT_TIMEOUT=5000

# Pump.fun Configuration
PUMPFUN_PROGRAM_ID=6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P

# Auto-Buy Configuration (for token sniper)
AUTO_BUY_ENABLED=false
AUTO_BUY_AMOUNT=0.01
```

## Provider Requirements

This module requires a gRPC provider that supports Solana's Geyser plugin interface. Recommendations:

- [Helius](https://helius.xyz/) - Reliable gRPC service with free tier
- [Shyft](https://shyft.to/) - Good for development and production
- [Chainstack](https://chainstack.com/) - Enterprise-grade infrastructure

## Notes

- The gRPC streams provide lower latency than regular WebSocket connections
- For production deployments, consider implementing more robust error handling and logging
- The reconnection logic will attempt to restore the stream if it's interrupted