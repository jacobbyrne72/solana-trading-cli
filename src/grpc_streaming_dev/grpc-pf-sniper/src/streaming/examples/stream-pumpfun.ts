import { PublicKey } from "@solana/web3.js";
import { GrpcStreamingClient } from "../client";
import { logger } from "../../utils/logger";
import dotenv from 'dotenv';
import { CommitmentLevel } from "@triton-one/yellowstone-grpc";

// Load environment variables
dotenv.config();

// Get configuration from environment
const GRPC_URL = process.env.YELLOWSTONE_GRPC_URL || "mainnet.helius-rpc.com:443";
const GRPC_XTOKEN = process.env.HELIUS_API_KEY || '';
const PUMPFUN_PROGRAM_ID = process.env.PUMPFUN_PROGRAM_ID || "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

/**
 * Example of how to stream pump.fun token transactions
 */
async function streamPumpFunTokens() {
  // Validate required environment variables
  if (!GRPC_XTOKEN) {
    logger.error("Please set HELIUS_API_KEY in your .env file");
    process.exit(1);
  }

  if (!GRPC_URL) {
    logger.error("YELLOWSTONE_GRPC_URL is not set or empty");
    process.exit(1);
  }

  logger.info(`Using gRPC URL: ${GRPC_URL}`);
  logger.info(`Using Helius API Key: ${GRPC_XTOKEN ? '****' + GRPC_XTOKEN.slice(-5) : 'Not set'}`);
  logger.info(`Monitoring pump.fun program: ${PUMPFUN_PROGRAM_ID}`);

  try {
    // Initialize the streaming client
    const streamingClient = new GrpcStreamingClient(GRPC_URL, GRPC_XTOKEN, true);
    
    // Check connection
    const version = await streamingClient.checkConnection();
    logger.info(`Connected to gRPC server version: ${JSON.stringify(version)}`);
    
    // Create a custom subscription for pump.fun transactions
    const request = {
      accounts: {},
      slots: {},
      transactions: {
        pumpdotfun: {
          vote: false,
          failed: false,
          signature: undefined,
          accountInclude: [PUMPFUN_PROGRAM_ID],
          accountExclude: [],
          accountRequired: [],
        },
      },
      transactionsStatus: {},
      entry: {},
      blocks: {},
      blocksMeta: {},
      accountsDataSlice: [],
      ping: undefined,
      commitment: CommitmentLevel.PROCESSED,
    };
    
    // Set up the subscription with callbacks
    await streamingClient.customSubscribe(
      request,
      (data) => {
        // Handle transaction data
        if (data.transaction) {
          // Convert buffer data to Base58 strings for easier handling
          const convertedTx = GrpcStreamingClient.convertBuffersToBase58(data.transaction);
          const slot = convertedTx.slot;
          logger.info(`Received transaction in slot: ${slot}`);
          
          try {
            const signature = convertedTx.transaction.signature;
            logger.info(`Transaction signature: ${signature}`);
            logger.info(`Transaction URL: https://solscan.io/tx/${signature}`);
            
            // Check if this is a token creation transaction
            if (convertedTx.transaction.meta && 
                convertedTx.transaction.meta.postTokenBalances && 
                convertedTx.transaction.meta.postTokenBalances.length > 0) {
              
              const tokenMint = convertedTx.transaction.meta.postTokenBalances[0].mint;
              logger.info(`Pump.fun token detected: ${tokenMint}`);
              logger.info(`Token URL: https://pump.fun/${tokenMint}`);
              
              // Here you could add logic to automatically buy or track the token
            } else {
              logger.info("Transaction involves pump.fun but doesn't contain token creation data");
            }
          } catch (error) {
            logger.error("Error processing transaction:", error);
          }
        } else if (data.pong) {
          logger.debug("Received pong from server");
        }
      },
      (error) => {
        // Handle errors
        logger.error("Subscription error:", error);
      }
    );
    
    logger.info("Pump.fun monitoring started. Press Ctrl+C to exit.");
  } catch (error) {
    logger.error("Error setting up pump.fun monitoring:", error);
  }
}

// Run the example if this file is executed directly
if (require.main === module) {
  streamPumpFunTokens();
}

export { streamPumpFunTokens };