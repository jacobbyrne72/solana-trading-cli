import { PublicKey } from "@solana/web3.js";
import { GrpcStreamingClient } from "../client";
import { logger } from "../../utils/logger";
import dotenv from 'dotenv';
import { CommitmentLevel } from "@triton-one/yellowstone-grpc";
import { PumpFunSDK } from "../../pumpdotfun-sdk/src/pumpfun";
import { AnchorProvider, Wallet } from "@coral-xyz/anchor";
import { Connection, Keypair, LAMPORTS_PER_SOL } from "@solana/web3.js";
import { getOrCreateKeypair } from "../../utils";

// Load environment variables
dotenv.config();

// Get configuration from environment
const GRPC_URL = process.env.YELLOWSTONE_GRPC_URL || "mainnet.helius-rpc.com:443";
const GRPC_XTOKEN = process.env.HELIUS_API_KEY || '';
const PUMPFUN_PROGRAM_ID = process.env.PUMPFUN_PROGRAM_ID || "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const RPC_URL = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
const AUTO_BUY_AMOUNT = (process.env.AUTO_BUY_AMOUNT || '0.01');
const AUTO_BUY_ENABLED = process.env.AUTO_BUY_ENABLED === 'true';
const SLIPPAGE_BASIS_POINTS = 500n; // 5%

// Track processed tokens to avoid duplicates
const processedTokens = new Set<string>();

/**
 * Advanced example showing how to snipe newly created tokens
 */
async function sniperBot() {
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
  logger.info(`Auto-buy enabled: ${AUTO_BUY_ENABLED}`);
  logger.info(`Auto-buy amount: ${AUTO_BUY_AMOUNT} SOL`);

  // Initialize connection and wallet
  let connection: Connection;
  let wallet: Keypair;
  let provider: AnchorProvider;
  let pumpFunSDK: PumpFunSDK;

  try {
    // Initialize SDK
    connection = new Connection(RPC_URL);
    wallet = await getOrCreateKeypair("./wallet.json");
    provider = new AnchorProvider(
      connection,
      new Wallet(wallet),
      AnchorProvider.defaultOptions()
    );
    
    pumpFunSDK = new PumpFunSDK(provider);
    logger.info(`Initialized PumpFunSDK with wallet ${wallet.publicKey.toString()}`);
    
    // Check wallet balance
    const balance = await connection.getBalance(wallet.publicKey);
    logger.info(`Wallet balance: ${balance / LAMPORTS_PER_SOL} SOL`);
    
    if (balance < parseFloat(AUTO_BUY_AMOUNT) * LAMPORTS_PER_SOL) {
      logger.warn(`Insufficient balance for auto-buying. Need at least ${AUTO_BUY_AMOUNT} SOL`);
    }
    
    // Initialize the streaming client
    const streamingClient = new GrpcStreamingClient(GRPC_URL, GRPC_XTOKEN, true);
    
    // Check connection
    const version = await streamingClient.checkConnection();
    logger.info(`Connected to gRPC server version: ${JSON.stringify(version)}`);
    
    // Create subscription request
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
    
    // Start streaming
    await streamingClient.customSubscribe(
      request,
      async (data) => {
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
              
              // Skip if we've already processed this token
              if (processedTokens.has(tokenMint)) {
                logger.info(`Already processed token ${tokenMint}. Skipping.`);
                return;
              }
              
              logger.info(`New token detected: ${tokenMint}`);
              logger.info(`Token URL: https://pump.fun/${tokenMint}`);
              
              // Mark token as processed
              processedTokens.add(tokenMint);
              
              // Auto-buy the token
              if (AUTO_BUY_ENABLED) {
                await autoBuyToken(tokenMint, pumpFunSDK, wallet, connection);
              }
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
        logger.error("Stream error:", error);
      }
    );
    
    logger.info("Token sniper bot started. Press Ctrl+C to exit.");
  } catch (error) {
    logger.error("Error in sniper bot:", error);
  }
}

/**
 * Auto-buy a newly detected token
 */
async function autoBuyToken(
  tokenMint: string, 
  pumpFunSDK: PumpFunSDK, 
  wallet: Keypair, 
  connection: Connection
): Promise<string | null> {
  try {
    const mintAddress = new PublicKey(tokenMint);
    const buyAmount = BigInt(parseFloat(AUTO_BUY_AMOUNT) * LAMPORTS_PER_SOL);
    
    logger.info(`Attempting to buy ${AUTO_BUY_AMOUNT} SOL worth of token: ${tokenMint}`);
    
    // Create buy transaction
    const buyTx = await pumpFunSDK.buy(
      wallet,
      mintAddress,
      buyAmount,
      SLIPPAGE_BASIS_POINTS
    );
    
    // Send and confirm transaction
    const signature = await connection.sendTransaction(buyTx, [wallet]);
    logger.info(`Buy transaction sent: ${signature}`);
    logger.info(`Transaction URL: https://solscan.io/tx/${signature}`);
    
    await connection.confirmTransaction(signature);
    logger.info("Transaction confirmed");
    
    // Check new balance
    const tokenBalance = await pumpFunSDK.getBalance(wallet.publicKey, mintAddress);
    logger.info(`New token balance: ${tokenBalance}`);
    
    // You could implement auto-sell logic here
    // setupAutoSell(tokenMint, pumpFunSDK, wallet, connection);
    
    return signature;
  } catch (error) {
    logger.error(`Error auto-buying token ${tokenMint}:`, error);
    return null;
  }
}

/**
 * Example auto-sell function (not fully implemented)
 */
function setupAutoSell(
  tokenMint: string, 
  pumpFunSDK: PumpFunSDK, 
  wallet: Keypair, 
  connection: Connection
): void {
  const timeoutMs = 60 * 1000; // 1 minute timeout
  logger.info(`Setting up auto-sell for ${tokenMint} after ${timeoutMs/1000} seconds`);
  
  setTimeout(async () => {
    try {
      logger.info(`Auto-selling ${tokenMint}...`);
      // Implement sell logic here
      // You would need to implement the sell method in your PumpFunSDK
      
      // Example (not implemented):
      // const sellTx = await pumpFunSDK.sell(wallet, new PublicKey(tokenMint));
      // const signature = await connection.sendTransaction(sellTx, [wallet]);
      // logger.info(`Sell transaction sent: ${signature}`);
      
      logger.info(`Auto-sell for ${tokenMint} complete.`);
    } catch (error) {
      logger.error(`Error auto-selling token ${tokenMint}:`, error);
    }
  }, timeoutMs);
}

// Run the bot if this file is executed directly
if (require.main === module) {
  sniperBot();
}

export { sniperBot, autoBuyToken };