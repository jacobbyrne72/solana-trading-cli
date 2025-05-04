import { CommitmentLevel } from "@triton-one/yellowstone-grpc";
import Client from "@triton-one/yellowstone-grpc";
import { PublicKey } from "@solana/web3.js";
import bs58 from "bs58";
import { logger } from "../utils/logger";
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

// Configuration constants
const PING_INTERVAL = 15000; // 15 seconds
const RECONNECT_TIMEOUT = 5000; // 5 seconds

/**
 * GrpcStreamingClient - A class for handling gRPC streaming connections to Solana
 * Provides methods for subscribing to account, transaction, and slot updates
 */
export class GrpcStreamingClient {
  private client: Client;
  private url: string;
  private token: string;
  private reconnectOnError: boolean;
  
  /**
   * Creates a new GrpcStreamingClient instance
   * 
   * @param url - The gRPC endpoint URL
   * @param token - The authentication token for the gRPC service
   * @param reconnectOnError - Whether to automatically attempt reconnection on errors
   */
  constructor(url: string, token: string, reconnectOnError = true) {
    this.url = url;
    this.token = token;
    this.reconnectOnError = reconnectOnError;
    this.client = new Client(url, token);
    
    logger.info(`GrpcStreamingClient initialized with URL: ${url}`);
  }
  
  /**
   * Check if the gRPC service is available by getting its version
   * 
   * @returns Promise resolving to the version info
   */
  async checkConnection(): Promise<any> {
    try {
      const version = await this.client.getVersion();
      logger.info(`Connected to gRPC server: ${JSON.stringify(version)}`);
      return version;
    } catch (error) {
      logger.error("Failed to connect to gRPC server", error);
      throw error;
    }
  }
  
  /**
   * Creates a subscription to monitor a program's transactions
   * 
   * @param programId - The program ID to monitor
   * @param callback - Function to call with transaction data
   * @param errorCallback - Function to call on stream errors
   * @param commitment - Transaction commitment level (default: PROCESSED for lowest latency)
   */
  async subscribeToProgram(
    programId: string | PublicKey, 
    callback: (data: any) => void,
    errorCallback?: (error: any) => void,
    commitment: CommitmentLevel = CommitmentLevel.PROCESSED
  ): Promise<void> {
    const programIdStr = typeof programId === 'string' ? programId : programId.toBase58();
    logger.info(`Subscribing to program: ${programIdStr}`);
    
    // Create subscription request
    const request = {
      accounts: {},
      slots: {},
      transactions: {
        program: {
          vote: false,
          failed: false,
          signature: undefined,
          accountInclude: [programIdStr],
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
      commitment: commitment,
    };
    
    await this.setupSubscription(request, callback, errorCallback);
  }
  
  /**
   * Creates a subscription to monitor specific accounts
   * 
   * @param accounts - Array of account addresses to monitor
   * @param callback - Function to call with account update data
   * @param errorCallback - Function to call on stream errors
   * @param commitment - Transaction commitment level (default: PROCESSED for lowest latency)
   */
  async subscribeToAccounts(
    accounts: (string | PublicKey)[],
    callback: (data: any) => void,
    errorCallback?: (error: any) => void,
    commitment: CommitmentLevel = CommitmentLevel.PROCESSED
  ): Promise<void> {
    const accountsStr = accounts.map(acc => typeof acc === 'string' ? acc : acc.toBase58());
    logger.info(`Subscribing to accounts: ${accountsStr.join(', ')}`);
    
    // Build accounts object for request
    const accountsObj: any = {};
    accountsStr.forEach(account => {
      accountsObj[account] = { encoding: 'base64' };
    });
    
    // Create subscription request
    const request = {
      accounts: accountsObj,
      slots: {},
      transactions: {},
      transactionsStatus: {},
      entry: {},
      blocks: {},
      blocksMeta: {},
      accountsDataSlice: [],
      ping: undefined,
      commitment: commitment,
    };
    
    await this.setupSubscription(request, callback, errorCallback);
  }
  
  /**
   * Creates a subscription to monitor slot updates
   * 
   * @param callback - Function to call with slot data
   * @param errorCallback - Function to call on stream errors
   * @param commitment - Commitment level (default: PROCESSED for lowest latency)
   */
  async subscribeToSlots(
    callback: (data: any) => void,
    errorCallback?: (error: any) => void,
    commitment: CommitmentLevel = CommitmentLevel.PROCESSED
  ): Promise<void> {
    logger.info(`Subscribing to slots with commitment: ${commitment}`);
    
    // Create subscription request
    const request = {
      slots: { slotRange: { startSlot: '0' } },
      accounts: {},
      transactions: {},
      transactionsStatus: {},
      entry: {},
      blocks: {},
      blocksMeta: {},
      accountsDataSlice: [],
      ping: undefined,
      commitment: commitment,
    };
    
    await this.setupSubscription(request, callback, errorCallback);
  }
  
  /**
   * Creates a custom subscription with a user-defined request object
   * 
   * @param request - The subscription request object
   * @param callback - Function to call with data
   * @param errorCallback - Function to call on stream errors
   */
  async customSubscribe(
    request: any,
    callback: (data: any) => void,
    errorCallback?: (error: any) => void
  ): Promise<void> {
    logger.info(`Setting up custom subscription`);
    await this.setupSubscription(request, callback, errorCallback);
  }
  
  /**
   * Utility method to set up a subscription stream
   * 
   * @param request - The subscription request
   * @param callback - Callback for data
   * @param errorCallback - Callback for errors
   */
  private async setupSubscription(
    request: any,
    callback: (data: any) => void,
    errorCallback?: (error: any) => void
  ): Promise<void> {
    const setupStream = async () => {
      try {
        const stream = await this.client.subscribe();
        
        // Handle data events
        stream.on("data", (data: any) => {
          try {
            callback(data);
          } catch (callbackError) {
            logger.error("Error in callback:", callbackError);
          }
        });
        
        // Handle error events
        stream.on("error", (error: any) => {
          logger.error("Stream error:", error);
          
          if (errorCallback) {
            try {
              errorCallback(error);
            } catch (callbackError) {
              logger.error("Error in error callback:", callbackError);
            }
          }
          
          // Attempt reconnection if enabled
          if (this.reconnectOnError) {
            logger.info(`Attempting to reconnect in ${RECONNECT_TIMEOUT/1000} seconds...`);
            setTimeout(() => {
              setupStream().catch(e => {
                logger.error("Failed to reconnect:", e);
              });
            }, RECONNECT_TIMEOUT);
          }
        });
        
        // Set up stream closed handler
        const streamClosed = new Promise<void>((resolve, reject) => {
          stream.on("end", () => {
            logger.info("Stream ended");
            resolve();
          });
          
          stream.on("close", () => {
            logger.info("Stream closed");
            resolve();
          });
        });
        
        // Send subscription request
        await new Promise<void>((resolve, reject) => {
          stream.write(request, (err: any) => {
            if (err === null || err === undefined) {
              resolve();
            } else {
              reject(err);
            }
          });
        }).catch((reason) => {
          logger.error("Error writing to stream:", reason);
          throw reason;
        });
        
        // Set up ping interval to keep connection alive
        const pingInterval = setInterval(() => {
          const pingRequest = {
            ping: { id: Date.now() },
            accounts: {},
            accountsDataSlice: [],
            transactions: {},
            blocks: {},
            blocksMeta: {},
            entry: {},
            slots: {},
          };
          
          stream.write(pingRequest, (err: any) => {
            if (err) {
              logger.error("Error sending ping:", err);
              clearInterval(pingInterval);
            }
          });
        }, PING_INTERVAL);
        
        // Clean up interval when stream closes
        streamClosed.then(() => {
          clearInterval(pingInterval);
          
          // Attempt reconnection if enabled
          if (this.reconnectOnError) {
            logger.info(`Stream closed. Attempting to reconnect in ${RECONNECT_TIMEOUT/1000} seconds...`);
            setTimeout(() => {
              setupStream().catch(e => {
                logger.error("Failed to reconnect:", e);
              });
            }, RECONNECT_TIMEOUT);
          }
        });
        
        logger.info("Subscription set up successfully");
      } catch (error) {
        logger.error("Error setting up subscription:", error);
        
        if (errorCallback) {
          try {
            errorCallback(error);
          } catch (callbackError) {
            logger.error("Error in error callback:", callbackError);
          }
        }
        
        // Attempt reconnection if enabled
        if (this.reconnectOnError) {
          logger.info(`Failed to set up subscription. Attempting to reconnect in ${RECONNECT_TIMEOUT/1000} seconds...`);
          setTimeout(() => {
            setupStream().catch(e => {
              logger.error("Failed to reconnect:", e);
            });
          }, RECONNECT_TIMEOUT);
        } else {
          throw error;
        }
      }
    };
    
    // Start the initial stream setup
    await setupStream();
  }
  
  /**
   * Utility function to convert Buffer objects to Base58 strings
   * 
   * @param obj - Object potentially containing Buffer data
   * @returns Object with Buffer data converted to Base58 strings
   */
  static convertBuffersToBase58(obj: any): any {
    if (obj === null || obj === undefined) {
      return obj;
    }
    
    // Handle Buffer objects
    if (obj.type === 'Buffer' && Array.isArray(obj.data)) {
      return bs58.encode(new Uint8Array(obj.data));
    }
    
    // Handle arrays
    if (Array.isArray(obj)) {
      return obj.map(item => GrpcStreamingClient.convertBuffersToBase58(item));
    }
    
    // Handle objects
    if (typeof obj === 'object') {
      // Handle Uint8Array directly
      if (obj instanceof Uint8Array) {
        return bs58.encode(obj);
      }
      
      const converted: any = {};
      for (const [key, value] of Object.entries(obj)) {
        // Skip certain number/string fields
        if (key === 'uiAmount' || key === 'decimals' || key === 'uiAmountString') {
          converted[key] = value;
        } else {
          converted[key] = GrpcStreamingClient.convertBuffersToBase58(value);
        }
      }
      return converted;
    }
    
    return obj;
  }
  
  /**
   * Get the latest blockhash using a gRPC call
   * @returns Promise resolving to the latest blockhash info
   */
  async getLatestBlockhash(): Promise<any> {
    try {
      return await this.client.getLatestBlockhash();
    } catch (error) {
      logger.error("Error getting latest blockhash:", error);
      throw error;
    }
  }
  
  /**
   * Get the current slot number using a gRPC call
   * @returns Promise resolving to the current slot number
   */
  async getCurrentSlot(): Promise<any> {
    try {
      return await this.client.getSlot();
    } catch (error) {
      logger.error("Error getting current slot:", error);
      throw error;
    }
  }
}