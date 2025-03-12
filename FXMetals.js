require("dotenv").config();
const WebSocket = require("ws");
const axios = require("axios");

const DERIV_API_URL = "wss://ws.derivws.com/websockets/v3?app_id=69284"; // Replace app_id if needed
const TELEGRAM_API = `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const FOREX_PAIRS = [
  "frxEURUSD", "frxUSDJPY", "frxXAUUSD"
];

// Symbol-specific configurations for fine-tuned analysis

// Symbol-specific configurations for fine-tuned analysis
const SYMBOL_CONFIGS = {
  "frxEURUSD": {
    shortPeriod: 9,
    longPeriod: 26,
    trendThreshold: 0.0002
  },
  "frxUSDJPY": {
    shortPeriod: 8,
    longPeriod: 22,
    trendThreshold: 0.02
  },
  "frxXAUUSD": {
    shortPeriod: 10,
    longPeriod: 30,
    trendThreshold: 0.5
  }
};

// Enhanced range thresholds with more granular control
const RANGE_THRESHOLDS = {
  "frxEURUSD": 0.0003,
  "frxUSDJPY": 0.03,
  "frxXAUUSD": 1.0
};

// Track pending requests
const pendingRequests = {};
const FVG_HISTORY = {};

// Use let instead of const for ws to allow reassignment during reconnection
let ws;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY_MS = 5000;

/**
 * Initialize WebSocket connection
 */
function initializeWebSocket() {
  ws = new WebSocket(DERIV_API_URL);
  
  ws.onopen = () => {
    console.log("Connected to Deriv API");
    reconnectAttempts = 0;
    FOREX_PAIRS.forEach(symbol => {
      requestCandles(symbol);
      subscribeToCandles(symbol);
    });
  };
  
  const lastCandleRequest = {}; // Track last candle request per symbol

  ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
  
      if (data.history && data.history.candles) {
          // ✅ Received historical candle data, process trend analysis
          console.log(`📊 Received candle update for ${data.echo_req.ticks_history}`);
          analyzeTrend(data.echo_req.ticks_history, data.history.candles.map(c => c.close));
  
      } else if (data.candles) {
          // ✅ Direct candle response (some responses may use this)
          console.log(`📊 Direct candle update for ${data.echo_req.ticks_history}`);
          analyzeTrend(data.echo_req.ticks_history, data.candles.map(c => c.close));
  
      } else if (data.tick) {
          // ✅ Live tick received, request candle updates every 60 seconds per symbol
          console.log(`📈 Tick received for ${data.tick.symbol}: ${data.tick.quote}`);
  
          const symbol = data.tick.symbol;
          if (!lastCandleRequest[symbol] || Date.now() - lastCandleRequest[symbol] >= 60000) {
              console.log(`🔄 Requesting candle update for ${symbol}`);
              requestCandles(symbol);
              lastCandleRequest[symbol] = Date.now(); // Track last request time
          }
  
      } else if (data.error) {
          console.error(`❌ API Error:`, data.error);
      } else {
          console.warn("⚠ Unexpected response structure:", data);
      }
  };
  
  
  ws.onerror = (error) => console.error("WebSocket Error:", error);
  
  ws.onclose = (event) => {
    console.log(`Disconnected from Deriv API (code: ${event.code}), reconnecting...`);
    reconnectWithBackoff();
  };
}

/**
 * Reconnect with exponential backoff
 */
function reconnectWithBackoff() {
  reconnectAttempts++;
  
  if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
    console.error(`Failed to reconnect after ${MAX_RECONNECT_ATTEMPTS} attempts. Please check network or API status.`);
    return;
  }
  
  // Calculate backoff time (exponential with jitter)
  const jitter = Math.random() * 0.3 + 0.85; // Random factor between 0.85-1.15
  const delay = Math.min(RECONNECT_DELAY_MS * Math.pow(1.5, reconnectAttempts - 1) * jitter, 60000);
  
  console.log(`Reconnection attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${Math.round(delay/1000)}s`);
  
  setTimeout(() => {
    try {
      initializeWebSocket();
    } catch (error) {
      console.error("Failed to initialize WebSocket:", error);
      reconnectWithBackoff();
    }
  }, delay);
}

/**
 * Send enhanced Telegram alerts with rich formatting and detailed analysis
 * 
 * @param {string|Object} message - Simple text message or structured analysis object
 * @param {Object} options - Optional configuration for the alert
 * @returns {Promise} - Promise from the axios request
 */
 // Stores significant RSI bounce points

// SMA CROSSING CODE
// Add these variables to store SMA data
let smaHistory = {};
let lastSMACrossState = {};

// Function to update SMA values and check for crossovers
function updateSMAData(symbol, price, timestamp) {
  // Initialize data structures if needed
  if (!smaHistory[symbol]) {
      smaHistory[symbol] = {
          prices: [],
          sma50: null,
          sma100: null,
          sma200: null,
          timestamps: [],
          lastMinute: null,
          isReliable: false
      };
      
      lastSMACrossState[symbol] = {
          sma50_above_sma100: null,
          sma50_above_sma200: null,
          sma100_above_sma200: null,
          lastAlertTime: 0
      };
  }
  
  // Extract the minute timestamp (floor to nearest minute)
  const currentMinute = Math.floor(timestamp / 60000) * 60000;
  
  // Check if this is a new minute
  if (currentMinute !== smaHistory[symbol].lastMinute) {
      smaHistory[symbol].prices.push(price);
      smaHistory[symbol].timestamps.push(currentMinute);
      smaHistory[symbol].lastMinute = currentMinute;
      
      // Keep only necessary history for longest SMA plus some buffer
      const maxLength = 200 + 10; // 200 for longest SMA + 10 buffer
      if (smaHistory[symbol].prices.length > maxLength) {
          smaHistory[symbol].prices.shift();
          smaHistory[symbol].timestamps.shift();
      }
      
      // Calculate SMAs if we have enough data
      const prices = smaHistory[symbol].prices;
      
      if (prices.length >= 50) {
          smaHistory[symbol].sma50 = calculateSMA(prices, 50);
      }
      
      if (prices.length >= 100) {
          smaHistory[symbol].sma100 = calculateSMA(prices, 100);
      }
      
      if (prices.length >= 200) {
          smaHistory[symbol].sma200 = calculateSMA(prices, 200);
          smaHistory[symbol].isReliable = true;
      }
      
      // Check for crossovers if we have all SMAs and data is reliable
      if (smaHistory[symbol].isReliable) {
          const crossoverResult = checkSMACrossovers(symbol);
          return crossoverResult ? crossoverResult : { sma50: smaHistory[symbol].sma50, sma100: smaHistory[symbol].sma100, sma200: smaHistory[symbol].sma200 };
      }
  } else {
      // Same minute update - could update the last price instead
      const lastIndex = smaHistory[symbol].prices.length - 1;
      if (lastIndex >= 0) {
          smaHistory[symbol].prices[lastIndex] = price;
      }
  }
  
  return { sma50: smaHistory[symbol].sma50, sma100: smaHistory[symbol].sma100, sma200: smaHistory[symbol].sma200 };
}

// Calculate Simple Moving Average
function calculateSMA(prices, period) {
  if (prices.length < period) return null;
  
  const relevantPrices = prices.slice(-period);
  const validPrices = relevantPrices.filter(price => typeof price === 'number' && isFinite(price) && !isNaN(price));
  
  if (validPrices.length < period) {
      console.warn(`Not enough valid prices for SMA${period} calculation`);
      return null;
  }
  
  return validPrices.reduce((total, price) => total + price, 0) / period;
}


// Example crossover detection function (since original wasn't provided)
function checkSMACrossovers(symbol) {
    const sma = smaHistory[symbol];
    const state = lastSMACrossState[symbol];
    const currentTime = Date.now();
    
    // Only proceed if we have all SMAs
    if (!sma.sma50 || !sma.sma100 || !sma.sma200) {
        return null;
    }
    
    // Determine current cross states
    const sma50_above_sma100 = sma.sma50 > sma.sma100;
    const sma50_above_sma200 = sma.sma50 > sma.sma200;
    const sma100_above_sma200 = sma.sma100 > sma.sma200;
    
    // Check for crossovers (state changes)
    const crossovers = [];
    
    // Only check for crossovers if we have previous state
    if (state.sma50_above_sma100 !== null) {
        // sma50 crosses above sma100
        if (sma50_above_sma100 && !state.sma50_above_sma100) {
            crossovers.push({
                type: 'CROSS_ABOVE',
                fast: 'sma50',
                slow: 'sma100',
                symbol: symbol,
                timestamp: currentTime
            });
        }
        // sma50 crosses below sma100
        else if (!sma50_above_sma100 && state.sma50_above_sma100) {
            crossovers.push({
                type: 'CROSS_BELOW',
                fast: 'sma50',
                slow: 'sma100',
                symbol: symbol,
                timestamp: currentTime
            });
        }
        
        // Check other crossovers similarly...
        // sma50 vs sma200
        if (sma50_above_sma200 && !state.sma50_above_sma200) {
            crossovers.push({
                type: 'CROSS_ABOVE',
                fast: 'sma50',
                slow: 'sma200',
                symbol: symbol,
                timestamp: currentTime
            });
        }
        else if (!sma50_above_sma200 && state.sma50_above_sma200) {
            crossovers.push({
                type: 'CROSS_BELOW',
                fast: 'sma50',
                slow: 'sma200',
                symbol: symbol,
                timestamp: currentTime
            });
        }
        
        // sma100 vs sma200
        if (sma100_above_sma200 && !state.sma100_above_sma200) {
            crossovers.push({
                type: 'CROSS_ABOVE',
                fast: 'sma100',
                slow: 'sma200',
                symbol: symbol,
                timestamp: currentTime
            });
        }
        else if (!sma100_above_sma200 && state.sma100_above_sma200) {
            crossovers.push({
                type: 'CROSS_BELOW',
                fast: 'sma100',
                slow: 'sma200',
                symbol: symbol,
                timestamp: currentTime
            });
        }
    }
    
    // Update state
    state.sma50_above_sma100 = sma50_above_sma100;
    state.sma50_above_sma200 = sma50_above_sma200;
    state.sma100_above_sma200 = sma100_above_sma200;
    
    // Return any detected crossovers
    return crossovers.length > 0 ? crossovers : null;
}
// Check for SMA crossovers
function checkSMACrossovers(symbol) {
    const alerts = [];
    const smaData = smaHistory[symbol];
    const crossState = lastSMACrossState[symbol];

    // Need all three SMAs to check crossovers and data must be reliable
    if (!smaData.sma50 || !smaData.sma100 || !smaData.sma200 || !smaData.isReliable) {
        return alerts;
    }

    // Current crossing states
    const sma50_above_sma100 = smaData.sma50 > smaData.sma100;
    const sma50_above_sma200 = smaData.sma50 > smaData.sma200;
    const sma100_above_sma200 = smaData.sma100 > smaData.sma200;

    // Only alert if not too soon after last alert (prevent alert spam)
    const now = Date.now();
    const hoursSinceLastAlert = (now - crossState.lastAlertTime) / (1000 * 60 * 60);

    // Check if we have previous state information
    if (crossState.sma50_above_sma100 !== null) {
        if (hoursSinceLastAlert >= 1) { // At least 1 hour between alerts
            // Check sma50 crossing sma100
            if (crossState.sma50_above_sma100 !== sma50_above_sma100) {
                const crossoverMessage = `sma50 ${sma50_above_sma100 ? 'crossed above' : 'crossed below'} sma100`;
                alerts.push({
                    type: sma50_above_sma100 ? "bullish_cross" : "bearish_cross",
                    message: crossoverMessage,
                    sma50: smaData.sma50.toFixed(2),
                    sma100: smaData.sma100.toFixed(2),
                    timestamp: now,
                    symbol: symbol
                });
                sendTelegramAlert({
                    symbol: symbol,
                    trend: sma50_above_sma100 ? "Bullish" : "Bearish",
                    signal: sma50_above_sma100 ? "BULLISH_CROSS" : "BEARISH_CROSS",
                    message: crossoverMessage,
                });
                crossState.lastAlertTime = now;
            }

            // Check sma50 crossing sma200
            if (crossState.sma50_above_sma200 !== sma50_above_sma200) {
                const crossoverMessage = `sma50 ${sma50_above_sma200 ? 'crossed above' : 'crossed below'} sma200`;
                alerts.push({
                    type: sma50_above_sma200 ? "bullish_cross" : "bearish_cross",
                    message: crossoverMessage,
                    sma50: smaData.sma50.toFixed(2),
                    sma200: smaData.sma200.toFixed(2),
                    timestamp: now,
                    symbol: symbol
                });
                sendTelegramAlert({
                    symbol: symbol,
                    trend: sma50_above_sma200 ? "Bullish" : "Bearish",
                    signal: sma50_above_sma200 ? "BULLISH_CROSS" : "BEARISH_CROSS",
                    message: crossoverMessage,
                });
                crossState.lastAlertTime = now;
            }

            // Check sma100 crossing sma200
            if (crossState.sma100_above_sma200 !== sma100_above_sma200) {
                const crossoverMessage = `sma100 ${sma100_above_sma200 ? 'crossed above' : 'crossed below'} sma200`;
                alerts.push({
                    type: sma100_above_sma200 ? "bullish_cross" : "bearish_cross",
                    message: crossoverMessage,
                    sma100: smaData.sma100.toFixed(2),
                    sma200: smaData.sma200.toFixed(2),
                    timestamp: now,
                    symbol: symbol
                });
                sendTelegramAlert({
                    symbol: symbol,
                    trend: sma100_above_sma200 ? "Bullish" : "Bearish",
                    signal: sma100_above_sma200 ? "BULLISH_CROSS" : "BEARISH_CROSS",
                    message: crossoverMessage,
                });
                crossState.lastAlertTime = now;
            }
        }
    } else {
        // First time calculation, just log the states without sending alerts
        console.log(`Initial SMA states for ${symbol}: sma50${sma50_above_sma100 ? '>' : '<'}sma100, sma50${sma50_above_sma200 ? '>' : '<'}sma200, sma100${sma100_above_sma200 ? '>' : '<'}sma200`);
    }

    // Update crossing states
    crossState.sma50_above_sma100 = sma50_above_sma100;
    crossState.sma50_above_sma200 = sma50_above_sma200;
    crossState.sma100_above_sma200 = sma100_above_sma200;

    return alerts;
}


function visualizeSMAState(symbol) {
    if (!smaHistory[symbol]) {
        console.log("No SMA data available");
        return;
    }
    
    console.log(`\n=== SMA Values for ${symbol} ===`);
    console.log(`sma50: ${smaHistory[symbol].sma50?.toFixed(2) || 'N/A'}`);
    console.log(`sma100: ${smaHistory[symbol].sma100?.toFixed(2) || 'N/A'}`);
    console.log(`sma200: ${smaHistory[symbol].sma200?.toFixed(2) || 'N/A'}`);
    
    const crossState = lastSMACrossState[symbol];
    if (crossState.sma50_above_sma100 !== null) {
        console.log(`sma50 is ${crossState.sma50_above_sma100 ? 'ABOVE' : 'BELOW'} sma100`);
    }
    if (crossState.sma50_above_sma200 !== null) {
        console.log(`sma50 is ${crossState.sma50_above_sma200 ? 'ABOVE' : 'BELOW'} sma200`);
    }
    if (crossState.sma100_above_sma200 !== null) {
        console.log(`sma100 is ${crossState.sma100_above_sma200 ? 'ABOVE' : 'BELOW'} sma200`);
    }
    console.log("----------------------------------------");
}

//END OF SMA CODE

// Store RSI history and significant points with validation tracking
let rsiHistory = [];
let significantRSIPoints = [];
let rsiSignalHistory = {}; // To track outcomes of previous signals

// Function to update RSI history and detect significant points
function updateRSIHistory(symbol, rsi, price) {
    // Add new RSI value to history, keeping a reasonable length
    if (!rsiHistory[symbol]) {
        rsiHistory[symbol] = [];
        significantRSIPoints[symbol] = [];
        rsiSignalHistory[symbol] = {};
    }
    
    rsiHistory[symbol].push({rsi, price, timestamp: Date.now()});
    if (rsiHistory[symbol].length > 200) rsiHistory[symbol].shift(); // Keep more history for validation
    
    // Need at least 3 points to detect patterns
    if (rsiHistory[symbol].length < 3) return [];
    
    // Check for new significant points
    const n = rsiHistory[symbol].length;
    // Look at the third-last point to allow confirmation of a pattern
    const checkIndex = n - 3;
    
    if (checkIndex < 1) return significantRSIPoints[symbol];
    
    const prevRSI = rsiHistory[symbol][checkIndex - 1].rsi;
    const currRSI = rsiHistory[symbol][checkIndex].rsi;
    const nextRSI = rsiHistory[symbol][checkIndex + 1].rsi;
    const latestRSI = rsiHistory[symbol][n - 1].rsi;
    
    // Detect bottom reversal (RSI went down then up - bullish reversal)
    if (currRSI < prevRSI && currRSI < nextRSI && nextRSI < rsiHistory[symbol][checkIndex + 2].rsi) {
        // Confirm it's a significant reversal (min 3% swing)
        const swingSize = (nextRSI - currRSI) / currRSI * 100;
        if (swingSize >= 3) {
            // Calculate historical performance for this RSI level
            const successRate = calculateRSIReversalSuccessRate(symbol, currRSI, "bottom_reversal");
            
            const newPoint = {
                value: currRSI,
                type: "bottom_reversal",
                timestamp: rsiHistory[symbol][checkIndex].timestamp,
                price: rsiHistory[symbol][checkIndex].price,
                successRate: successRate,
                hitCount: 1,  // Initialize hit count
                lastAlerted: 0  // Initialize lastAlerted timestamp
            };
            
            // Check if this point is unique enough
            if (!significantRSIPoints[symbol].some(p => 
                p.type === "bottom_reversal" && Math.abs(p.value - currRSI) < 2)) {
                significantRSIPoints[symbol].push(newPoint);
                console.log(`New RSI bottom reversal detected at ${currRSI} with ${successRate.successes}/${successRate.total} historical success rate`);
                
                // Store this signal for future validation
                trackSignalForValidation(symbol, newPoint);
            }
        }
    }
    
    // Detect top reversal (RSI went up then down - bearish reversal)
    if (currRSI > prevRSI && currRSI > nextRSI && nextRSI > rsiHistory[symbol][checkIndex + 2].rsi) {
        // Confirm it's a significant reversal (min 3% swing)
        const swingSize = (currRSI - nextRSI) / currRSI * 100;
        if (swingSize >= 3) {
            // Calculate historical performance for this RSI level
            const successRate = calculateRSIReversalSuccessRate(symbol, currRSI, "top_reversal");
            
            const newPoint = {
                value: currRSI,
                type: "top_reversal",
                timestamp: rsiHistory[symbol][checkIndex].timestamp,
                price: rsiHistory[symbol][checkIndex].price,
                successRate: successRate,
                hitCount: 1,  // Initialize hit count
                lastAlerted: 0  // Initialize lastAlerted timestamp
            };
            
            // Check if this point is unique enough
            if (!significantRSIPoints[symbol].some(p => 
                p.type === "top_reversal" && Math.abs(p.value - currRSI) < 2)) {
                significantRSIPoints[symbol].push(newPoint);
                console.log(`New RSI top reversal detected at ${currRSI} with ${successRate.successes}/${successRate.total} historical success rate`);
                
                // Store this signal for future validation
                trackSignalForValidation(symbol, newPoint);
            }
        }
    }
    
    // Validate past signals based on price movement
    validatePastSignals(symbol);
    
    return significantRSIPoints[symbol];
}

// Calculate success rate for RSI reversals at similar levels with margin of error
function calculateRSIReversalSuccessRate(symbol, currentRSI, reversalType) {
    if (!rsiSignalHistory[symbol]) return { successes: 0, total: 0, adjustedRate: 0.5 };
    
    // Find similar RSI levels (within ±1.5 points)
    const similarSignals = Object.values(rsiSignalHistory[symbol]).filter(signal => 
        signal.type === reversalType && 
        Math.abs(signal.rsiValue - currentRSI) <= 1.5 &&
        signal.validated === true
    );
    
    // Count successful reversals
    const successfulSignals = similarSignals.filter(signal => signal.successful === true);
    const successCount = successfulSignals.length;
    const totalCount = similarSignals.length;
    
    // Apply Wilson score interval for small sample sizes
    // This gives a lower bound estimate that's more conservative when sample size is small
    let adjustedRate = 0.5; // Default to neutral when no data
    
    if (totalCount > 0) {
        // Simple success rate
        const rawRate = successCount / totalCount;
        
        // Confidence level z-score (95% confidence = 1.96)
        const z = 1.96;
        
        // Wilson score interval lower bound - more conservative estimate
        const denominator = 1 + (z * z / totalCount);
        const numerator = rawRate + (z * z / (2 * totalCount)) - 
                          z * Math.sqrt((rawRate * (1 - rawRate) + (z * z / (4 * totalCount))) / totalCount);
        
        adjustedRate = numerator / denominator;
        
        // Additional penalty for very small sample sizes
        if (totalCount < 5) {
            // Blend with 0.5 (neutral) based on sample size
            adjustedRate = (adjustedRate * totalCount + 0.5 * (5 - totalCount)) / 5;
        }
    }
    
    return {
        successes: successCount,
        total: totalCount,
        rawRate: totalCount > 0 ? successCount / totalCount : 0.5,
        adjustedRate: adjustedRate,
        confidence: Math.min(1, Math.sqrt(totalCount) / 5) // Confidence in the rate based on sample size
    };
}

// Track a new signal for future validation
function trackSignalForValidation(symbol, signalPoint) {
    const signalId = `${signalPoint.type}_${signalPoint.value.toFixed(1)}_${Date.now()}`;
    
    rsiSignalHistory[symbol][signalId] = {
        id: signalId,
        timestamp: Date.now(),
        rsiValue: signalPoint.value,
        price: signalPoint.price,
        type: signalPoint.type,
        validated: false,
        successful: null,
        validationTimestamp: null,
        priceChange: null
    };
}

// Validate past signals based on subsequent price movement
function validatePastSignals(symbol) {
    const now = Date.now();
    const pendingValidation = Object.values(rsiSignalHistory[symbol]).filter(signal => 
        !signal.validated && 
        now - signal.timestamp >= 30 * 60 * 1000 // Validate after 30 minutes
    );
    
    pendingValidation.forEach(signal => {
        // Find current price
        const currentPrice = rsiHistory[symbol][rsiHistory[symbol].length - 1].price;
        
        // Calculate price change
        const priceChange = ((currentPrice - signal.price) / signal.price) * 100;
        
        // Determine if signal was successful
        let successful = false;
        if (signal.type === "bottom_reversal" && priceChange > 0.5) {
            successful = true;
        } else if (signal.type === "top_reversal" && priceChange < -0.5) {
            successful = true;
        }
        
        // Update signal record
        rsiSignalHistory[symbol][signal.id].validated = true;
        rsiSignalHistory[symbol][signal.id].successful = successful;
        rsiSignalHistory[symbol][signal.id].validationTimestamp = now;
        rsiSignalHistory[symbol][signal.id].priceChange = priceChange;
        
        console.log(`Validated ${signal.type} signal at RSI ${signal.rsiValue.toFixed(1)}: ${successful ? 'Successful' : 'Failed'} (${priceChange.toFixed(2)}% price change)`);
    });
    
    // Cleanup old records - keep only the last 100 validated signals
    const allSignals = Object.values(rsiSignalHistory[symbol])
        .filter(signal => signal.validated)
        .sort((a, b) => b.validationTimestamp - a.validationTimestamp);
    
    if (allSignals.length > 100) {
        const toRemove = allSignals.slice(100);
        toRemove.forEach(signal => {
            delete rsiSignalHistory[symbol][signal.id];
        });
    }
}

// Function to check if current RSI is near a significant point
function checkRSICrossing(symbol, currentRSI, currentPrice) {
    const alerts = [];
    
    if (!significantRSIPoints[symbol]) return alerts;
    
    significantRSIPoints[symbol].forEach(point => {
        // Check if current RSI is revisiting a significant level (within ±1.5 points)
        if (Math.abs(currentRSI - point.value) <= 1.5) {
            // Only alert if we haven't recently alerted about this point
            const hoursSinceAlert = point.lastAlerted ? (Date.now() - point.lastAlerted) / (1000 * 60 * 60) : 24; // Default to 24 if never alerted
            
            if (hoursSinceAlert > 2) { // Don't alert more than once every 2 hours
                // Increment the hit count for this point
                point.hitCount = (point.hitCount || 1) + 1;
                point.lastAlerted = Date.now();
                
                // Get historical success rate with margin of error
                const successRate = calculateRSIReversalSuccessRate(symbol, point.value, point.type);
                
                alerts.push({
                    type: point.type,
                    value: point.value,
                    current: currentRSI,
                    hitCount: point.hitCount,
                    successRate: successRate,
                    message: `RSI revisiting ${point.type === "bottom_reversal" ? "bullish" : "bearish"} reversal point at ${point.value.toFixed(1)} (hit count: ${point.hitCount}, historical success: ${successRate.successes}/${successRate.total}, adjusted: ${(successRate.adjustedRate * 100).toFixed(1)}%)`
                });
            }
            if (alerts.length > 0) {
                // A peak was revisited, visualize the current state
                visualizeRSIPeaks(symbol);
            }
        }
    });
    
    return alerts;
}

// Function to visualize RSI peaks and revisits (for debugging/monitoring)
function visualizeRSIPeaks(symbol) {
    if (!significantRSIPoints[symbol] || significantRSIPoints[symbol].length === 0) {
        console.log("No significant RSI points to visualize");
        return;
    }
    
    console.log(`\n=== Significant RSI Points for ${symbol} ===`);
    console.log("Type\t\tRSI Value\tHit Count\tSuccess Rate");
    console.log("--------------------------------------------------------");
    
    significantRSIPoints[symbol].forEach(point => {
        const successRate = calculateRSIReversalSuccessRate(symbol, point.value, point.type);
        console.log(`${point.type === "bottom_reversal" ? "Bottom (↑)" : "Top (↓)"}\t${point.value.toFixed(1)}\t\t${point.hitCount || 1}\t\t${successRate.successes}/${successRate.total} (${(successRate.adjustedRate * 100).toFixed(1)}%)`);
    });
    console.log("--------------------------------------------------------");
}

function sendTelegramAlert(message, options = {}) {
  const config = {
    useMarkdown: false, // No Markdown
    includeTechnicals: true,
    includeChartUrl: true,
    rateLimitMs: 1000,
    ...options
};

if (!sendTelegramAlert.lastSentTime) {
    sendTelegramAlert.lastSentTime = 0;
    sendTelegramAlert.pendingQueue = [];
}

const isAnalysisObject = typeof message === 'object' && message !== null;
let formattedMessage;

if (isAnalysisObject) {
    let rsiEmoji = '⚪';
    if (message.rsi > 70) rsiEmoji = '🔴';
    else if (message.rsi < 30) rsiEmoji = '🟢';

    let volatilityLevel = 'Low';
    if (message.volatility > 0.05) volatilityLevel = 'High';
    else if (message.volatility > 0.02) volatilityLevel = 'Medium';
    // Retrieve SMA values for the given symbol
    const smaData = smaHistory[message.symbol] || {};
    const crossState = lastSMACrossState[message.symbol] || {};

    // Start constructing the message
    formattedMessage = `📊 ${message.symbol.slice(3)} | ${message.trend}\n`;
    formattedMessage += `Confidence: ${message.confidence}% | Range: ${message.priceRange} pips\n`;
    formattedMessage += `RSI: ${rsiEmoji} ${message.rsi} | Volatility: ${volatilityLevel} (${message.volatility})\n`;

    // Add SMA data
    formattedMessage += `\nSMA Values for ${message.symbol.slice(3)}\n`;
    formattedMessage += `sma50: ${smaData.sma50?.toFixed(2) || 'N/A'}\n`;
    formattedMessage += `sma100: ${smaData.sma100?.toFixed(2) || 'N/A'}\n`;
    formattedMessage += `sma200: ${smaData.sma200?.toFixed(2) || 'N/A'}\n`;
    
        if (message.fvgSignals && message.fvgSignals.length > 0) {
          formattedMessage += `\n🔍 FVG OPPORTUNITIES:\n`;
          
          // Sort by priority (highest first)
          const sortedFVG = [...message.fvgSignals].sort((a, b) => b.priority - a.priority);
          
          // Add each FVG alert (limit to top 2 to avoid overly long messages)
          sortedFVG.slice(0, 2).forEach((fvg, index) => {
              const directionEmoji = fvg.type === 'bullish' ? '🟢' : '🔴';
              const priorityStars = '⭐'.repeat(Math.min(Math.round(fvg.priority / 2), 5));
              
              formattedMessage += `${directionEmoji} ${fvg.type.toUpperCase()} FVG ${priorityStars}\n`;
              formattedMessage += `Range: ${fvg.range}\n`;
              
              // Add success rate if available
              if (message.fvgData && message.fvgData.history && message.fvgData.history.successRate) {
                  const successRate = message.fvgData.history.successRate[fvg.type];
                  if (successRate > 0) {
                      const percentage = Math.round(successRate * 100);
                      formattedMessage += `Success rate: ${percentage}%\n`;
                  }
              }
              
              // Add separator between multiple FVGs
              if (index < Math.min(sortedFVG.length, 2) - 1) {
                  formattedMessage += `\n`;
              }
          });
      }
   

        

        // Add crossover state information
        if (crossState.sma50_above_sma100 !== null) {
            formattedMessage += `sma50 is ${crossState.sma50_above_sma100 ? 'ABOVE' : 'BELOW'} sma100\n`;
        }
        if (crossState.sma50_above_sma200 !== null) {
            formattedMessage += `sma50 is ${crossState.sma50_above_sma200 ? 'ABOVE' : 'BELOW'} sma200\n`;
        }
        if (crossState.sma100_above_sma200 !== null) {
            formattedMessage += `sma100 is ${crossState.sma100_above_sma200 ? 'ABOVE' : 'BELOW'} sma200\n`;
        }


        // Add RSI alerts if present
        if (message.rsiAlerts && message.rsiAlerts.length > 0) {
            formattedMessage += `\nRSI Alerts:\n`;
            message.rsiAlerts.forEach(alert => {
                formattedMessage += `- ${alert}\n`;
            });
        }

        if (message.signal !== "NEUTRAL") {
            formattedMessage +=  `⚠️ Signal: *${message.signal}*\n`;
        } if (config.includeChartUrl) {
          formattedMessage += `\nChart: https://tradingview.com/chart?symbol=${encodeURIComponent(message.symbol.slice(3))}`;

      }
      } else {
          // FVG-specific alert format (for dedicated FVG alerts)
          if (typeof message === 'string' && message.includes('FVG')) {
              // For FVG-specific alerts, keep the original message which already has formatting
              formattedMessage = message;
          } else {
              // For other string messages, use as is
              formattedMessage = message;
          }
        }
        

    // Ensure the message is within Telegram's limit of 4000 characters
    if (formattedMessage.length > 4000) {
        formattedMessage = formattedMessage.substring(0, 3997) + '...';
    }

    const payload = {
        chat_id: CHAT_ID,
        text: formattedMessage,
        parse_mode: config.useMarkdown ? 'Markdown' : undefined,
        disable_web_page_preview: !config.includeChartUrl
    };

    const now = Date.now();
    const timeSinceLastAlert = now - sendTelegramAlert.lastSentTime;

    if (timeSinceLastAlert < config.rateLimitMs) {
        sendTelegramAlert.pendingQueue.push({ payload, sendTime: now + (config.rateLimitMs - timeSinceLastAlert) });
        setTimeout(processAlertQueue, config.rateLimitMs - timeSinceLastAlert);
        return;
    }

    sendTelegramAlert.lastSentTime = now;
    console.log("Sending Telegram message:", payload);
    return axios.post(TELEGRAM_API, payload)
        .catch(error => {
            console.error("Telegram Error:", error.response?.data || error.message);
            return Promise.reject(error);
        });
}



  

/**
 * Process the queue of pending alerts
 */
function processAlertQueue() {
  const now = Date.now();
  const { pendingQueue } = sendTelegramAlert;
  
  // Find messages ready to send
  const readyToSend = pendingQueue.filter(item => item.sendTime <= now);
  
  if (readyToSend.length > 0) {
    // Update the queue to remove messages we're about to send
    sendTelegramAlert.pendingQueue = pendingQueue.filter(item => item.sendTime > now);
    
    // Send the first ready message
    const { payload, resolve, reject } = readyToSend[0];
    sendTelegramAlert.lastSentTime = now;
    console.log("Sending Telegram message:", payload);
    
    axios.post(TELEGRAM_API, payload)
      .then(response => {
        resolve(response);
        
        // If we have more messages, schedule the next one
        if (readyToSend.length > 1 || sendTelegramAlert.pendingQueue.length > 0) {
          setTimeout(processAlertQueue, 1000);
        }
      })
      .catch(error => {
   
        console.error("Telegram Error:", error.response?.data || error.message);
        
        // Continue processing queue despite error
        if (readyToSend.length > 1 || sendTelegramAlert.pendingQueue.length > 0) {
          setTimeout(processAlertQueue, 1000);
        }
      });
  }
}

/**
 * Format analysis object into a readable Telegram message with Markdown
 */
function formatAnalysisMessage(analysis, config) {
  // Escape special characters for MarkdownV2 format
  const escape = (text) => {
    if (!config.useMarkdown) return text;
    return String(text).replace(/[_*[\]()~`>#+=|{}.!-]/g, '\\$&');
  };
  
  // Create signal emoji based on trend and signal
  let signalEmoji = '📊';
  if (analysis.trend === 'UPTREND') {
    signalEmoji = analysis.signal === 'OVERBOUGHT' ? '⚠️📈' : '📈';
  } else if (analysis.trend === 'DOWNTREND') {
    signalEmoji = analysis.signal === 'OVERSOLD' ? '⚠️📉' : '📉';
  } else if (analysis.signal === 'SUPPORT') {
    signalEmoji = '🔍📊';
  } else if (analysis.signal === 'RESISTANCE') {
    signalEmoji = '🔍📊';
  }
  
  // Generate Telegram message with Markdown formatting
  let msg = '';
  
  // Header with symbol and primary signal
  msg += `${signalEmoji} *${escape(analysis.symbol)}* \\| `;
  
  if (analysis.signal !== 'NEUTRAL' && analysis.signal !== undefined) {
    msg += `*${escape(analysis.signal)}* \\| `;
  }
  
  msg += `*${escape(analysis.trend)}*\n`;
  
  // Confidence and range info
  msg += `Confidence: ${escape(Math.round(analysis.confidence * 100))}%`;
  
  if (analysis.priceRange !== undefined) {
    msg += ` \\| Range: ${escape(analysis.priceRange)} pips`;
  }
  
  // Technical indicators section
  if (config.includeTechnicals && analysis.indicators) {
    const ind = analysis.indicators;
    msg += '\n\n*Technical Indicators:*\n';
    
    if (ind.rsi !== undefined) {
      const rsiEmoji = ind.rsi > 70 ? '🔴' : (ind.rsi < 30 ? '🟢' : '⚪');
      msg += `RSI: ${rsiEmoji} ${escape(Math.round(ind.rsi))}\n`;
    }
    
    if (ind.maShort !== undefined && ind.maLong !== undefined) {
      const maDiff = ((ind.maShort - ind.maLong) / ind.maLong * 100).toFixed(2);
      const direction = ind.maShort > ind.maLong ? '↗️' : '↘️';
      msg += `MA Trend: ${direction} ${escape(maDiff)}%\n`;
    }
    
    if (analysis.volatility !== undefined) {
      const volLevel = analysis.volatility > 0.05 ? 'High' : (analysis.volatility > 0.02 ? 'Medium' : 'Low');
      msg += `Volatility: ${escape(volLevel)} (${escape(analysis.volatility.toFixed(4))})\n`;
    }
  }
  
  // Add chart URL if enabled
  if (config.includeChartUrl) {
    const chartUrl = `https://charts.deriv.com/?symbol=${analysis.symbol}`;
    msg += `\n[View Chart](${chartUrl})`;
  }
  
  // Add timestamp
  const now = new Date();
  const timestamp = now.toISOString().replace('T', ' ').substring(0, 19);
  msg += `\n\n_${escape(timestamp)}_`;
  
  return msg;
}

/**
 * Advanced trend analysis function for trading signals
 * Includes multiple indicators, adaptive thresholds, and noise filtering
 * 
 * @param {string} symbol - Trading symbol identifier
 * @param {number[]} prices - Array of historical prices
 * @param {Object} options - Optional configuration parameters
 * @returns {Object} Analysis results with trend information and confidence metrics
 */
function analyzeTrend(symbol, prices, options = {}) {
    // Validate inputs and apply defaults
    if (!Array.isArray(prices) || prices.length < 50) {
      console.warn(`Insufficient data for ${symbol}: need at least 50 price points`);
      return { 
        symbol, 
        trend: "UNKNOWN", 
        confidence: 0, 
        message: "Insufficient data for analysis" 
      };
    }
  
    // Default parameters with symbol-specific overrides
    const config = {
      shortPeriod: options.shortPeriod || 5,
      longPeriod: options.longPeriod || 20,
      veryLongPeriod: options.veryLongPeriod || 50,
      trendThreshold: options.trendThreshold || 0.0008,
      strongTrendThreshold: options.strongTrendThreshold || 0.002,
      volatilityWindow: options.volatilityWindow || 14,
      ...SYMBOL_CONFIGS[symbol] || {}
    };
  
    // Calculate volatility-adjusted range threshold
    const candles = prices.map(close => ({
        high: close,  // Assuming high == close
        low: close,   // Assuming low == close
        close: close
      }));
      const volatility = calculateVolatility(candles, config.volatilityWindow);
      const vixIndicator = calculateVIXVolatility(candles);

    const rangeThreshold = RANGE_THRESHOLDS[symbol] || Math.max(0.0008, volatility * (symbol.startsWith('R_') ? 1.2 : 1.0));
  
    // Extract clean price data (handle potential NaN or invalid values)
    const cleanPrices = prices.filter(price => typeof price === 'number' && !isNaN(price));
    if (cleanPrices.length < 50) {
      console.warn(`Clean data for ${symbol} insufficient after filtering: ${cleanPrices.length} points`);
      return { 
        symbol, 
        trend: "UNKNOWN", 
        confidence: 0, 
        message: "Data quality issues detected" 
      };
    }
  
    // Calculate advanced indicators
    const indicators = calculateIndicators(cleanPrices, config);
    const currentPrice = cleanPrices[cleanPrices.length - 1];
    const timestamp = Date.now();

    // Update RSI history and track historical performance
    updateRSIHistory(symbol, indicators.rsi, currentPrice);

    // Update and check SMA crossovers (new addition)
    const smaCrossingAlerts = updateSMAData(symbol, currentPrice, timestamp);

    // Check for FVG opportunities - NEW FVG INTEGRATION
    const fvgAlerts = checkFVGSignals(symbol, candles, { symbol });
    
    // Update FVG history and evaluate performance - NEW FVG INTEGRATION
    fvgAlerts.forEach(alert => updateFVGHistory(symbol, alert, currentPrice));
    evaluateFVGPerformance(symbol, currentPrice);

    const priceStats = calculatePriceStats(cleanPrices);

    // Trend determination with confidence score
    let { trend, confidence, signal } = determineTrend(indicators, config, volatility);

    // Check for RSI crossing significant levels with historical success rate
    const rsiCrossingAlerts = checkRSICrossing(symbol, indicators.rsi, currentPrice);

    // Prepare detailed analysis result
    const analysis = {
      symbol,
      trend,
      confidence: Math.round(confidence * 100) / 100,
      signal,
      priceRange: Math.round(priceStats.range * 100) / 100,
      volatility: Math.round(volatility * 10000) / 10000,
      rangeThreshold,
      indicators: {
        maShort: Math.round(indicators.maShort * 100) / 100,
        maLong: Math.round(indicators.maLong * 100) / 100,
        maVeryLong: Math.round(indicators.maVeryLong * 100) / 100,
        rsi: Math.round(indicators.rsi * 10) / 10,
        momentum: Math.round(indicators.momentum * 1000) / 1000
      },
      // Include FVG data in analysis result - NEW FVG INTEGRATION
      fvgData: {
        alerts: fvgAlerts,
        history: FVG_HISTORY[symbol] || { successRate: { bullish: 0, bearish: 0 } }
      }
    };

// Handle RSI crossing alerts
if (rsiCrossingAlerts.length > 0) {
  const bullishAlerts = rsiCrossingAlerts.filter(a => a.type === "bottom_reversal");
  const bearishAlerts = rsiCrossingAlerts.filter(a => a.type === "top_reversal");

    

  
  // Enhance confidence based on historical success rate
  const enhanceConfidence = (alerts, symbol, signalType) => {
    let confidence = 0;
    
    // Base confidence from historical success (original logic)
    if (alerts.length > 0) {
      const bestAlert = alerts.reduce((best, current) => {
        return current.successRate.adjustedRate > best.successRate.adjustedRate ? current : best;
      }, alerts[0]);
      
      // Apply confidence boost based on historical success with confidence factor
      confidence += bestAlert.successRate.adjustedRate * bestAlert.successRate.confidence * 0.2;
    }
    
    // Add SMA crossover confirmation boost
    if (smaHistory[symbol] && lastSMACrossState[symbol]) {
      const crossState = lastSMACrossState[symbol];
      const smaData = smaHistory[symbol];
      
      // Only consider SMA data if we have all three SMAs calculated
      if (smaData.sma50 && smaData.sma100 && smaData.sma200) {
        let smaTrendStrength = 0;
        const isBullish = signalType === "bullish";
        
        // Check if crossover states align with signal direction
        if (crossState.sma50_above_sma100 === isBullish) {
          smaTrendStrength += 0.1; // 19 above 50 is bullish
        }
        
        if (vixIndicator.value > 0.05) {
            confidence += 0.1; // Increase confidence in strong trends
        }
        
        if (crossState.sma50_above_sma200 === isBullish) {
          smaTrendStrength += 0.15; // 19 above 100 is more significant
        }
        
        if (crossState.sma100_above_sma200 === isBullish) {
          smaTrendStrength += 0.15; // 50 above 100 is more significant (longer-term)
        }
        
        // Check how far the SMAs are from each other (magnitude of separation)
        const sma50_50_separation = Math.abs(smaData.sma50 - smaData.sma100) / smaData.sma100;
        const sma50_100_separation = Math.abs(smaData.sma50 - smaData.sma200) / smaData.sma200;
        
        // Add extra confidence if separation is significant but not too extreme
        if (sma50_50_separation > 0.005 && sma50_50_separation < 0.03) {
          smaTrendStrength += 0.05;
        }
        
        if (sma50_100_separation > 0.01 && sma50_100_separation < 0.05) {
          smaTrendStrength += 0.05;
        }
        
        // Recent crossover gives higher confidence (within last 3 data points)
        if (crossState.lastAlertTime && (Date.now() - crossState.lastAlertTime) < 3 * 60 * 1000) {
          smaTrendStrength += 0.1;
        }
        
        // Add SMA trend strength to total confidence
        confidence += smaTrendStrength;
      }
    }
    
    return confidence;
  };
  
  // Usage in your signal generation code:
  if (bullishAlerts.length > 0 && trend !== "DOWNTREND") {
    signal = "RSI_BULLISH_REVERSAL";
    confidence = Math.min(1, confidence + enhanceConfidence(bullishAlerts, symbol, "bullish"));
  } else if (bearishAlerts.length > 0 && trend !== "UPTREND") {
    signal = "RSI_BEARISH_REVERSAL";
    confidence = Math.min(1, confidence + enhanceConfidence(bearishAlerts, symbol, "bearish"));
  }
  
  if (bullishAlerts.length > 0 && trend !== "DOWNTREND") {
    signal = "RSI_BULLISH_REVERSAL";
    confidence = Math.min(1, confidence + enhanceConfidence(bullishAlerts));
  } else if (bearishAlerts.length > 0 && trend !== "UPTREND") {
    signal = "RSI_BEARISH_REVERSAL";
    confidence = Math.min(1, confidence + enhanceConfidence(bearishAlerts));
  }
}

// Handle SMA crossing alerts (new addition)
if (smaCrossingAlerts.length > 0) {
  const bullishSMACrosses = smaCrossingAlerts.filter(a => a.type === "bullish_cross");
  const bearishSMACrosses = smaCrossingAlerts.filter(a => a.type === "bearish_cross");
  
  // Modify signal based on SMA crossovers - prioritizing based on longer-term MAs
  if (bullishSMACrosses.length > 0) {
    // Find the most significant bullish cross
    const hassma100_100Cross = bullishSMACrosses.some(a => a.message.includes("sma100") && a.message.includes("sma200"));
    const hassma50_100Cross = bullishSMACrosses.some(a => a.message.includes("sma50") && a.message.includes("sma200"));
    
    if (hassma100_100Cross) {
      signal = "sma100_100_BULLISH_CROSS";
      confidence = Math.min(1, confidence + 0.15); // Boost confidence
    } else if (hassma50_100Cross) {
      signal = "sma50_100_BULLISH_CROSS";
      confidence = Math.min(1, confidence + 0.12);
    } else {
      signal = "sma50_50_BULLISH_CROSS";
      confidence = Math.min(1, confidence + 0.08);
    }
    
    // Strengthen trend if aligns with current trend
    if (trend !== "DOWNTREND") {
      trend = "UPTREND";
    }
  } else if (bearishSMACrosses.length > 0) {
    // Find the most significant bearish cross
    const hassma100_100Cross = bearishSMACrosses.some(a => a.message.includes("sma100") && a.message.includes("sma200"));
    const hassma50_100Cross = bearishSMACrosses.some(a => a.message.includes("sma50") && a.message.includes("sma200"));
    
    if (hassma100_100Cross) {
      signal = "sma100_100_BEARISH_CROSS";
      confidence = Math.min(1, confidence + 0.15);
    } else if (hassma50_100Cross) {
      signal = "sma50_100_BEARISH_CROSS";
      confidence = Math.min(1, confidence + 0.12);
    } else {
      signal = "sma50_50_BEARISH_CROSS";
      confidence = Math.min(1, confidence + 0.08);
    }
    
    // Strengthen trend if aligns with current trend
    if (trend !== "UPTREND") {
      trend = "DOWNTREND";
    }
  }
  
  // Add SMA data to analysis result
  analysis.smaCrossovers = smaCrossingAlerts.map(alert => alert.message);
}

// Prepare message for alerts
let message;
if (trend === "RANGE" && priceStats.range <= rangeThreshold) {
  message = `📊 ${symbol} is in a tight range (${priceStats.range} pips, RSI: ${Math.round(indicators.rsi)})`;
} else if (trend === "RANGE") {
  message = `📊 ${symbol} is ranging with wider swings (${priceStats.range} pips, RSI: ${Math.round(indicators.rsi)})`;
} else if (confidence >= 0.8) {
  message = `${trend === "UPTREND" ? "📈" : "📉"} ${symbol}: STRONG ${trend} (Conf: ${Math.round(confidence * 100)}%, RSI: ${Math.round(indicators.rsi)})`;
} else {
  message = `${trend === "UPTREND" ? "📈" : "📉"} ${symbol}: ${trend} (Conf: ${Math.round(confidence * 100)}%, RSI: ${Math.round(indicators.rsi)})`;
}

// Check for SMA crossovers and enhance the message (new addition)
if (smaCrossingAlerts.length > 0) {
  const crossType = smaCrossingAlerts.some(a => a.type === "bullish_cross") ? "bullish" : "bearish";
  message += ` - SMA ${crossType} crossover detected!`;
}

// Log and send alerts
console.log(message);

if (trend === "RANGE" || signal !== "NEUTRAL" || rsiCrossingAlerts.length > 0 || smaCrossingAlerts.length > 0) {
  const alertObject = {
    symbol,
    trend,
    confidence: Math.round(confidence * 100),
    signal,
    priceRange: Math.round(priceStats.range * 100) / 100,
    volatility: Math.round(volatility * 10000) / 10000,
    rsi: Math.round(indicators.rsi),
  };
  
  // Add RSI alerts with historical success rates if present
  if (rsiCrossingAlerts.length > 0) {
    alertObject.rsiAlerts = rsiCrossingAlerts.map(alert => {
      const historyText = alert.successRate.total > 0 ? 
        `(${alert.successRate.successes}/${alert.successRate.total} historical success)` : 
        "(insufficient historical data)";
      return `${alert.message} ${historyText}`;
    });
  }
  
  // Add SMA alerts if present (new addition)
  if (smaCrossingAlerts.length > 0) {
    alertObject.smaAlerts = smaCrossingAlerts.map(alert => alert.message);
  }


  if (fvgAlerts.length > 0) {
    // Add FVG signals to the result
    analysis.fvgSignals = fvgAlerts.map(alert => ({
      type: alert.type,
      priority: alert.priority,
      range: `${alert.gapDetails.gapStart} - ${alert.gapDetails.gapEnd}`,
      message: alert.message
    }));
    
    // Adjust confidence based on FVG signals
    const highPriorityFVG = fvgAlerts.filter(a => a.priority > 7);
    if (highPriorityFVG.length > 0) {
      // Adjust trend if strong FVG signals present
      const strongBullish = highPriorityFVG.filter(a => a.type === 'bullish').length;
      const strongBearish = highPriorityFVG.filter(a => a.type === 'bearish').length;
      
      if (strongBullish > strongBearish && trend !== 'UP') {
        analysis.trend = 'MIXED_BULLISH';
        analysis.message = `Strong bullish FVG detected, watch for potential reversal. ${analysis.message || ''}`;
      } else if (strongBearish > strongBullish && trend !== 'DOWN') {
        analysis.trend = 'MIXED_BEARISH';
        analysis.message = `Strong bearish FVG detected, watch for potential reversal. ${analysis.message || ''}`;
      }
    }
  }
  
  // Optional: Visualize current state for debugging
  visualizeRSIPeaks(symbol);
  visualizeSMAState(symbol);
  
  sendTelegramAlert(alertObject);
}

return { ...analysis, message }
}

/**
 * Calculate key technical indicators from price data
 */
function calculateIndicators(prices, config) {
  // Simple moving averages (efficient implementation)
  const maShort = calculateEMA(prices, config.shortPeriod);
  const maLong = calculateEMA(prices, config.longPeriod);
  const maVeryLong = calculateEMA(prices, config.veryLongPeriod);
  
  // Calculate RSI (Relative Strength Index)
  const rsi = calculateRSI(prices, 14);
  
  // Calculate price momentum
  const momentum = (prices[prices.length - 1] / prices[prices.length - 6]) - 1;
  
  // MACD calculation
  const macd = maShort - maLong;
  const signal = calculateEMA(
    prices.map((_, i, arr) => 
      i >= config.longPeriod - 1 ? 
        calculateEMA(arr.slice(0, i+1), config.shortPeriod) - calculateEMA(arr.slice(0, i+1), config.longPeriod) : 
        0
    ).slice(-9),
    9
  );
  
  return { maShort, maLong, maVeryLong, rsi, momentum, macd, macdSignal: signal };
}

/**
 * Calculate Exponential Moving Average (more responsive than SMA)
 */
function calculateEMA(prices, period) {
  if (prices.length < period) return null;
  
  const k = 2 / (period + 1);
  let ema = prices.slice(0, period).reduce((sum, price) => sum + price, 0) / period;
  
  for (let i = period; i < prices.length; i++) {
    ema = (prices[i] * k) + (ema * (1 - k));
  }
  
  return ema;
}

/**
 * Calculate RSI (Relative Strength Index)
 */
function calculateRSI(prices, period) {
  if (prices.length <= period) return 50; // Default neutral value
  
  let gains = 0;
  let losses = 0;
  
  // Calculate initial avg gain/loss
  for (let i = 1; i <= period; i++) {
    const change = prices[i] - prices[i - 1];
    if (change >= 0) {
      gains += change;
    } else {
      losses -= change; // Convert to positive
    }
  }
  
  let avgGain = gains / period;
  let avgLoss = losses / period;
  
  // Calculate subsequent values
  for (let i = period + 1; i < prices.length; i++) {
    const change = prices[i] - prices[i - 1];
    
    avgGain = ((avgGain * (period - 1)) + (change > 0 ? change : 0)) / period;
    avgLoss = ((avgLoss * (period - 1)) + (change < 0 ? -change : 0)) / period;
  }
  
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

/**
 * Calculate price volatility (ATR-inspired)
 */
function calculateVolatility(candles, period = 14, useEMA = true) {
  if (candles.length < period + 1) {
    return 0;
  }

  const trueRanges = [];
  
  for (let i = 1; i < candles.length; i++) {
    const highLowRange = candles[i].high - candles[i].low;
    const highClosePrev = Math.abs(candles[i].high - candles[i-1].close);
    const lowClosePrev = Math.abs(candles[i].low - candles[i-1].close);
    
    const trueRange = Math.max(highLowRange, highClosePrev, lowClosePrev);
    trueRanges.push(trueRange);
  }

  let atr;
  
  if (useEMA) {
    const multiplier = 2 / (period + 1);
    atr = trueRanges.slice(0, period).reduce((sum, tr) => sum + tr, 0) / period;
    
    for (let i = period; i < trueRanges.length; i++) {
      atr = (trueRanges[i] * multiplier) + (atr * (1 - multiplier));
    }
  } else {
    atr = trueRanges.slice(-period).reduce((sum, tr) => sum + tr, 0) / period;
  }

  const recentCloses = candles.slice(-period).map(candle => candle.close);
  const averagePrice = recentCloses.reduce((sum, price) => sum + price, 0) / recentCloses.length;
  
  return (atr / averagePrice) * 100;  
}

  /**
   * Enhanced volatility calculation with adaptive parameters for VIX indices
   * More suitable for 1-minute timeframes on VIX 10-100 derived indices
   * @param {Object[]} candles - Array of OHLC candles
   * @param {Object} options - Configuration options
   * @returns {Object} Volatility metrics
   */
  function calculateVIXVolatility(candles, options = {}) {
    const {
      period = 14,
      adaptivePeriod = true,
      scalingFactor = 100,
      vixThreshold = 30
    } = options;
    
    // Adaptive period - use shorter period when VIX is higher
    let effectivePeriod = period;
    if (adaptivePeriod) {
      const lastClose = candles[candles.length - 1].close;
      if (lastClose > vixThreshold) {
        // Reduce period when volatility is already high
        effectivePeriod = Math.max(5, Math.floor(period * vixThreshold / lastClose));
      }
    }
    
    const rawVolatility = calculateVolatility(candles, effectivePeriod, true);
    
    // Calculate rate of change in volatility (acceleration)
    const volatilityChange = candles.length > effectivePeriod * 2 ? 
      rawVolatility / calculateVolatility(candles.slice(0, -effectivePeriod), effectivePeriod, true) - 1 : 
      0;
    
    return {
      value: rawVolatility * scalingFactor,
      normalized: rawVolatility,
      change: volatilityChange,
      period: effectivePeriod
    };
  }


  

  /**
   * Identifies Fair Value Gap (FVG) opportunities in price data
   * @param {Array} candles - Array of candle objects with high, low, open, close properties
   * @param {Object} options - Configuration options
   * @returns {Array} - Array of identified FVG objects
   */
function identifyFVG(candles, options = {}) {
  // Default configuration parameters
  const config = {
    minGapSize: options.minGapSize || 0.0008, // Minimum gap size as fraction
    maxGapAge: options.maxGapAge || 20,       // Maximum candles to look back
    confirmationCandles: options.confirmationCandles || 2, // Candles needed to confirm FVG
    minPriceDistance: options.minPriceDistance || 0.0003, // Min distance from current price
    ...SYMBOL_CONFIGS[options.symbol] || {}
  };

  if (candles.length < 3) return [];
  
  const fvgResults = [];
  const totalCandles = candles.length;
  
  // Look for Bullish FVG (downside gap) and Bearish FVG (upside gap)
  for (let i = 2; i < totalCandles; i++) {
    const middleCandle = candles[i-1];
    const firstCandle = candles[i-2];
    const thirdCandle = candles[i];
    const currentPrice = candles[totalCandles-1].close;
    
    // Bullish FVG (downside gap)
    if (thirdCandle.low > middleCandle.high) {
      const gapSize = thirdCandle.low - middleCandle.high;
      const gapPercentage = gapSize / middleCandle.high;
      
      if (gapPercentage >= config.minGapSize && (totalCandles - i) <= config.maxGapAge) {
        // Calculate if price is approaching the gap
        const distanceToGap = Math.abs(currentPrice - middleCandle.high) / currentPrice;
        
        if (distanceToGap >= config.minPriceDistance) {
          fvgResults.push({
            type: 'bullish',
            candle: i,
            gapStart: middleCandle.high,
            gapEnd: thirdCandle.low,
            gapSize: gapPercentage,
            gapAge: totalCandles - i,
            distanceToPrice: distanceToGap,
            mitigated: false,
            priority: calculateFVGPriority('bullish', gapPercentage, totalCandles - i, distanceToGap)
          });
        }
      }
    }
    
    // Bearish FVG (upside gap)
    if (thirdCandle.high < middleCandle.low) {
      const gapSize = middleCandle.low - thirdCandle.high;
      const gapPercentage = gapSize / middleCandle.low;
      
      if (gapPercentage >= config.minGapSize && (totalCandles - i) <= config.maxGapAge) {
        // Calculate if price is approaching the gap
        const distanceToGap = Math.abs(currentPrice - middleCandle.low) / currentPrice;
        
        if (distanceToGap >= config.minPriceDistance) {
          fvgResults.push({
            type: 'bearish',
            candle: i,
            gapStart: thirdCandle.high,
            gapEnd: middleCandle.low,
            gapSize: gapPercentage,
            gapAge: totalCandles - i,
            distanceToPrice: distanceToGap,
            mitigated: false,
            priority: calculateFVGPriority('bearish', gapPercentage, totalCandles - i, distanceToGap)
          });
        }
      }
    }
  }
  
  // Check if any previously identified gaps have been mitigated
  for (let fvg of fvgResults) {
    if (fvg.type === 'bullish') {
      // Check if price has entered the gap from above
      for (let j = fvg.candle + 1; j < totalCandles; j++) {
        if (candles[j].low <= fvg.gapEnd && candles[j].low >= fvg.gapStart) {
          fvg.mitigated = true;
          break;
        }
      }
    } else { // bearish
      // Check if price has entered the gap from below
      for (let j = fvg.candle + 1; j < totalCandles; j++) {
        if (candles[j].high >= fvg.gapStart && candles[j].high <= fvg.gapEnd) {
          fvg.mitigated = true;
          break;
        }
      }
    }
  }
  
  // Filter out mitigated gaps and sort by priority
  return fvgResults
    .filter(fvg => !fvg.mitigated)
    .sort((a, b) => b.priority - a.priority);
}

/**
 * Calculate priority score for FVG based on multiple factors
 * @param {string} type - 'bullish' or 'bearish'
 * @param {number} gapSize - Size of the gap as percentage
 * @param {number} age - Age of the gap in candles
 * @param {number} distanceToPrice - Current distance to price
 * @returns {number} - Priority score
 */
function calculateFVGPriority(type, gapSize, age, distanceToPrice) {
  // Newer gaps with larger size are prioritized
  const sizeScore = gapSize * 100; // Convert percentage to points
  const ageScore = (20 - Math.min(age, 20)) / 20; // Newer is better, max age of 20
  const proximityScore = (0.01 - Math.min(distanceToPrice, 0.01)) * 100; // Closer is better
  
  // Combine scores with weights
  return (sizeScore * 0.5) + (ageScore * 0.3) + (proximityScore * 0.2);
}

/**
 * Check for FVG signals and generate alert messages
 * @param {string} symbol - Trading symbol
 * @param {Array} candles - Array of candle objects
 * @param {Object} options - Configuration options
 * @returns {Array} - Array of FVG alert objects
 */
function checkFVGSignals(symbol, candles, options = {}) {
  const fvgGaps = identifyFVG(candles, { ...options, symbol });
  const alerts = [];
  
  // Only process top priority gaps (limit to 3 max)
  const topGaps = fvgGaps.slice(0, 3);
  
  for (const gap of topGaps) {
    // Only alert if priority is significant
    if (gap.priority < 3) continue;
    
    const currentPrice = candles[candles.length - 1].close;
    const priceAction = gap.type === 'bullish' ? 
      "Potential bullish reversal from support" : 
      "Potential bearish reversal from resistance";
    
    // Format numbers for display
    const gapStartFormatted = Math.round(gap.gapStart * 100000) / 100000;
    const gapEndFormatted = Math.round(gap.gapEnd * 100000) / 100000;
    
    alerts.push({
      symbol,
      type: gap.type,
      message: `${symbol}: FVG ${gap.type.toUpperCase()} opportunity detected!
${priceAction}
Price range: ${gapStartFormatted} - ${gapEndFormatted}
Gap size: ${Math.round(gap.gapSize * 10000) / 100}%
Current price: ${Math.round(currentPrice * 100000) / 100000}
Priority: ${Math.round(gap.priority * 10) / 10}/10`,
      gapDetails: gap,
      priority: gap.priority,
      timestamp: Date.now()
    });
  }
  
  return alerts;
}

/**
 * Track FVG history and success rate
 * @param {string} symbol - Trading symbol
 * @param {Object} fvgAlert - FVG alert object
 * @param {number} currentPrice - Current price
 */
function updateFVGHistory(symbol, fvgAlert, currentPrice) {
  if (!FVG_HISTORY[symbol]) {
    FVG_HISTORY[symbol] = {
      alerts: [],
      successRate: { bullish: 0, bearish: 0 },
      totalAlerts: { bullish: 0, bearish: 0 },
      successfulAlerts: { bullish: 0, bearish: 0 }
    };
  }
  
  // Add new alert to history
  FVG_HISTORY[symbol].alerts.push({
    ...fvgAlert,
    priceAtAlert: currentPrice,
    status: 'active'
  });
  
  // Increment counter
  FVG_HISTORY[symbol].totalAlerts[fvgAlert.type]++;
  
  // Limit history size
  if (FVG_HISTORY[symbol].alerts.length > 50) {
    FVG_HISTORY[symbol].alerts.shift();
  }
}

/**
 * Update status of previous FVG alerts and calculate success rate
 * @param {string} symbol - Trading symbol
 * @param {number} currentPrice - Current price
 */
function evaluateFVGPerformance(symbol, currentPrice) {
  if (!FVG_HISTORY[symbol] || !FVG_HISTORY[symbol].alerts) return;
  
  const history = FVG_HISTORY[symbol];
  
  // Update status of active alerts
  for (const alert of history.alerts) {
    if (alert.status !== 'active') continue;
    
    const gap = alert.gapDetails;
    const timeSinceAlert = Date.now() - alert.timestamp;
    const hoursSinceAlert = timeSinceAlert / (1000 * 60 * 60);
    
    // Consider successful if price moved favorably by at least half the gap size
    if (gap.type === 'bullish') {
      if (currentPrice > alert.priceAtAlert * (1 + gap.gapSize * 0.5)) {
        alert.status = 'success';
        history.successfulAlerts.bullish++;
      } else if (currentPrice < Math.min(gap.gapStart, alert.priceAtAlert * 0.995) || hoursSinceAlert > 24) {
        alert.status = 'failed';
      }
    } else { // bearish
      if (currentPrice < alert.priceAtAlert * (1 - gap.gapSize * 0.5)) {
        alert.status = 'success';
        history.successfulAlerts.bearish++;
      } else if (currentPrice > Math.max(gap.gapEnd, alert.priceAtAlert * 1.005) || hoursSinceAlert > 24) {
        alert.status = 'failed';
      }
    }
  }
  
  // Calculate success rates
  if (history.totalAlerts.bullish > 0) {
    history.successRate.bullish = history.successfulAlerts.bullish / history.totalAlerts.bullish;
  }
  
  if (history.totalAlerts.bearish > 0) {
    history.successRate.bearish = history.successfulAlerts.bearish / history.totalAlerts.bearish;
  }
}

/**
 * Identifies Fair Value Gap (FVG) opportunities in price data
 * @param {Array} candles - Array of candle objects with high, low, open, close properties
 * @param {Object} options - Configuration options
 * @returns {Array} - Array of identified FVG objects
 */
function identifyFVG(candles, options = {}) {
  // Default configuration parameters
  const config = {
    minGapSize: options.minGapSize || 0.0008, // Minimum gap size as fraction
    maxGapAge: options.maxGapAge || 20,       // Maximum candles to look back
    confirmationCandles: options.confirmationCandles || 2, // Candles needed to confirm FVG
    minPriceDistance: options.minPriceDistance || 0.0003, // Min distance from current price
    ...SYMBOL_CONFIGS[options.symbol] || {}
  };

  if (candles.length < 3) return [];
  
  const fvgResults = [];
  const totalCandles = candles.length;
  
  // Look for Bullish FVG (downside gap) and Bearish FVG (upside gap)
  for (let i = 2; i < totalCandles; i++) {
    const middleCandle = candles[i-1];
    const firstCandle = candles[i-2];
    const thirdCandle = candles[i];
    const currentPrice = candles[totalCandles-1].close;
    
    // Bullish FVG (downside gap)
    if (thirdCandle.low > middleCandle.high) {
      const gapSize = thirdCandle.low - middleCandle.high;
      const gapPercentage = gapSize / middleCandle.high;
      
      if (gapPercentage >= config.minGapSize && (totalCandles - i) <= config.maxGapAge) {
        // Calculate if price is approaching the gap
        const distanceToGap = Math.abs(currentPrice - middleCandle.high) / currentPrice;
        
        if (distanceToGap >= config.minPriceDistance) {
          fvgResults.push({
            type: 'bullish',
            candle: i,
            gapStart: middleCandle.high,
            gapEnd: thirdCandle.low,
            gapSize: gapPercentage,
            gapAge: totalCandles - i,
            distanceToPrice: distanceToGap,
            mitigated: false,
            priority: calculateFVGPriority('bullish', gapPercentage, totalCandles - i, distanceToGap)
          });
        }
      }
    }
    
    // Bearish FVG (upside gap)
    if (thirdCandle.high < middleCandle.low) {
      const gapSize = middleCandle.low - thirdCandle.high;
      const gapPercentage = gapSize / middleCandle.low;
      
      if (gapPercentage >= config.minGapSize && (totalCandles - i) <= config.maxGapAge) {
        // Calculate if price is approaching the gap
        const distanceToGap = Math.abs(currentPrice - middleCandle.low) / currentPrice;
        
        if (distanceToGap >= config.minPriceDistance) {
          fvgResults.push({
            type: 'bearish',
            candle: i,
            gapStart: thirdCandle.high,
            gapEnd: middleCandle.low,
            gapSize: gapPercentage,
            gapAge: totalCandles - i,
            distanceToPrice: distanceToGap,
            mitigated: false,
            priority: calculateFVGPriority('bearish', gapPercentage, totalCandles - i, distanceToGap)
          });
        }
      }
    }
  }
  
  // Check if any previously identified gaps have been mitigated
  for (let fvg of fvgResults) {
    if (fvg.type === 'bullish') {
      // Check if price has entered the gap from above
      for (let j = fvg.candle + 1; j < totalCandles; j++) {
        if (candles[j].low <= fvg.gapEnd && candles[j].low >= fvg.gapStart) {
          fvg.mitigated = true;
          break;
        }
      }
    } else { // bearish
      // Check if price has entered the gap from below
      for (let j = fvg.candle + 1; j < totalCandles; j++) {
        if (candles[j].high >= fvg.gapStart && candles[j].high <= fvg.gapEnd) {
          fvg.mitigated = true;
          break;
        }
      }
    }
  }
  
  // Filter out mitigated gaps and sort by priority
  return fvgResults
    .filter(fvg => !fvg.mitigated)
    .sort((a, b) => b.priority - a.priority);
}

/**
 * Calculate priority score for FVG based on multiple factors
 * @param {string} type - 'bullish' or 'bearish'
 * @param {number} gapSize - Size of the gap as percentage
 * @param {number} age - Age of the gap in candles
 * @param {number} distanceToPrice - Current distance to price
 * @returns {number} - Priority score
 */
function calculateFVGPriority(type, gapSize, age, distanceToPrice) {
  // Newer gaps with larger size are prioritized
  const sizeScore = gapSize * 100; // Convert percentage to points
  const ageScore = (20 - Math.min(age, 20)) / 20; // Newer is better, max age of 20
  const proximityScore = (0.01 - Math.min(distanceToPrice, 0.01)) * 100; // Closer is better
  
  // Combine scores with weights
  return (sizeScore * 0.5) + (ageScore * 0.3) + (proximityScore * 0.2);
}

/**
 * Check for FVG signals and generate alert messages
 * @param {string} symbol - Trading symbol
 * @param {Array} candles - Array of candle objects
 * @param {Object} options - Configuration options
 * @returns {Array} - Array of FVG alert objects
 */
function checkFVGSignals(symbol, candles, options = {}) {
  const fvgGaps = identifyFVG(candles, { ...options, symbol });
  const alerts = [];
  
  // Only process top priority gaps (limit to 3 max)
  const topGaps = fvgGaps.slice(0, 3);
  
  for (const gap of topGaps) {
    // Only alert if priority is significant
    if (gap.priority < 3) continue;
    
    const currentPrice = candles[candles.length - 1].close;
    const priceAction = gap.type === 'bullish' ? 
      "Potential bullish reversal from support" : 
      "Potential bearish reversal from resistance";
    
    // Format numbers for display
    const gapStartFormatted = Math.round(gap.gapStart * 100000) / 100000;
    const gapEndFormatted = Math.round(gap.gapEnd * 100000) / 100000;
    
    alerts.push({
      symbol,
      type: gap.type,
      message: `${symbol}: FVG ${gap.type.toUpperCase()} opportunity detected!
${priceAction}
Price range: ${gapStartFormatted} - ${gapEndFormatted}
Gap size: ${Math.round(gap.gapSize * 10000) / 100}%
Current price: ${Math.round(currentPrice * 100000) / 100000}
Priority: ${Math.round(gap.priority * 10) / 10}/10`,
      gapDetails: gap,
      priority: gap.priority,
      timestamp: Date.now()
    });
  }
  
  return alerts;
}

/**
 * Track FVG history and success rate
 * @param {string} symbol - Trading symbol
 * @param {Object} fvgAlert - FVG alert object
 * @param {number} currentPrice - Current price
 */
function updateFVGHistory(symbol, fvgAlert, currentPrice) {
  if (!FVG_HISTORY[symbol]) {
    FVG_HISTORY[symbol] = {
      alerts: [],
      successRate: { bullish: 0, bearish: 0 },
      totalAlerts: { bullish: 0, bearish: 0 },
      successfulAlerts: { bullish: 0, bearish: 0 }
    };
  }
  
  // Add new alert to history
  FVG_HISTORY[symbol].alerts.push({
    ...fvgAlert,
    priceAtAlert: currentPrice,
    status: 'active'
  });
  
  // Increment counter
  FVG_HISTORY[symbol].totalAlerts[fvgAlert.type]++;
  
  // Limit history size
  if (FVG_HISTORY[symbol].alerts.length > 50) {
    FVG_HISTORY[symbol].alerts.shift();
  }
}

/**
 * Update status of previous FVG alerts and calculate success rate
 * @param {string} symbol - Trading symbol
 * @param {number} currentPrice - Current price
 */
function evaluateFVGPerformance(symbol, currentPrice) {
  if (!FVG_HISTORY[symbol] || !FVG_HISTORY[symbol].alerts) return;
  
  const history = FVG_HISTORY[symbol];
  
  // Update status of active alerts
  for (const alert of history.alerts) {
    if (alert.status !== 'active') continue;
    
    const gap = alert.gapDetails;
    const timeSinceAlert = Date.now() - alert.timestamp;
    const hoursSinceAlert = timeSinceAlert / (1000 * 60 * 60);
    
    // Consider successful if price moved favorably by at least half the gap size
    if (gap.type === 'bullish') {
      if (currentPrice > alert.priceAtAlert * (1 + gap.gapSize * 0.5)) {
        alert.status = 'success';
        history.successfulAlerts.bullish++;
      } else if (currentPrice < Math.min(gap.gapStart, alert.priceAtAlert * 0.995) || hoursSinceAlert > 24) {
        alert.status = 'failed';
      }
    } else { // bearish
      if (currentPrice < alert.priceAtAlert * (1 - gap.gapSize * 0.5)) {
        alert.status = 'success';
        history.successfulAlerts.bearish++;
      } else if (currentPrice > Math.max(gap.gapEnd, alert.priceAtAlert * 1.005) || hoursSinceAlert > 24) {
        alert.status = 'failed';
      }
    }
  }
  
  // Calculate success rates
  if (history.totalAlerts.bullish > 0) {
    history.successRate.bullish = history.successfulAlerts.bullish / history.totalAlerts.bullish;
  }
  
  if (history.totalAlerts.bearish > 0) {
    history.successRate.bearish = history.successfulAlerts.bearish / history.totalAlerts.bearish;
  }
}

/**
 * Calculate price statistics
 */
function calculatePriceStats(prices) {
  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const range = max - min;
  const current = prices[prices.length - 1];
  const percentFromLow = ((current - min) / range) * 100;
  
  return { min, max, range, current, percentFromLow };
}

/**
 * Determine trend with confidence score
 */
function determineTrend(indicators, config, volatility, rsiHistory) {
    const { maShort, maLong, maVeryLong, momentum, rsi, macd, macdSignal } = indicators;

    let trend = "RANGE";
    let confidence = 0;
    let signal = "NEUTRAL";

    // Identify RSI Significant Points
    const rsiSignificant = getRSISignificantPoints(rsiHistory);

    // Trend Strength Adjustments
    const maDiff = maShort - maLong;
    const maLongDiff = maLong - maVeryLong;
    const trendStrength = Math.abs(maDiff) / volatility; 
    const strongTrend = trendStrength > config.strongTrendThreshold;
    const weakTrend = trendStrength > config.trendThreshold;

    if (maShort > maLong && maLong > maVeryLong && momentum > 0) {
        confidence = calculateUptrendConfidence(maDiff, maLongDiff, rsi, momentum, macd, macdSignal, config.trendThreshold);
        trend = "UPTREND";

        // Boost confidence if RSI bounced off a significant level
        if (rsiSignificant.bouncedUpFrom30) confidence += 0.1;
        if (rsiSignificant.bouncedDownFrom50) confidence -= 0.05;

        signal = confidence >= 0.8 ? "BUY" : "NEUTRAL";
    } 
    else if (maShort < maLong && maLong < maVeryLong && momentum < 0) {
        confidence = calculateDowntrendConfidence(maDiff, maLongDiff, rsi, momentum, macd, macdSignal, config.trendThreshold);
        trend = "DOWNTREND";

        // Boost confidence if RSI bounced off a significant level
        if (rsiSignificant.bouncedDownFrom70) confidence += 0.1;
        if (rsiSignificant.bouncedUpFrom50) confidence -= 0.05;

        signal = confidence >= 0.8 ? "SELL" : "NEUTRAL";
    } 
    else if (Math.abs(momentum) < 0.0005 && rsi > 40 && rsi < 60) {
        trend = "RANGE";
        confidence = 0.3; // Lower confidence for range detection
    }

    return { trend, confidence: Math.min(1, confidence), signal };
}


function getRSISignificantPoints(rsiHistory) {
    if (!Array.isArray(rsiHistory) || rsiHistory.length < 3) return [];

    let significantPoints = [];

    for (let i = 1; i < rsiHistory.length - 1; i++) {
        const prevRSI = rsiHistory[i - 1];
        const currRSI = rsiHistory[i];
        const nextRSI = rsiHistory[i + 1];

        // Overbought/Oversold crossover detection
        if ((currRSI > 70 && prevRSI <= 70) || (currRSI < 30 && prevRSI >= 30)) {
            significantPoints.push({ index: i, value: currRSI, type: "crossover" });
        }

        // **Improved Bounce Detection**
        if (
            currRSI < prevRSI && currRSI < nextRSI && // Local minimum
            prevRSI - currRSI >= 2 && // Ensure a significant drop
            nextRSI - currRSI >= 2 // Ensure a meaningful bounce
        ) {
            significantPoints.push({ index: i, value: currRSI, type: "bounce" });
        }
    }

    return significantPoints;
}





/**
 * Calculate uptrend confidence
 */
function calculateUptrendConfidence(maDiff, maLongDiff, rsi, momentum, macd, macdSignal, threshold) {
  let confidence = 0;
  
  // Moving average alignment (40%)
  confidence += Math.min(1, maDiff / (threshold * 4)) * 0.4;
  
  // RSI confirmation (20%)
  const rsiScore = (rsi - 50) / 20; // 50-70 scaled to 0-1
  confidence += Math.max(0, Math.min(1, rsiScore)) * 0.2;
  
  // Momentum confirmation (20%)
  confidence += Math.max(0, Math.min(1, momentum * 50)) * 0.2;
  
  // Long-term trend alignment (20%)
  confidence += (maLongDiff > 0 ? Math.min(1, maLongDiff / (threshold * 2)) : 0) * 0.2;
  
  // MACD confirmation - bonus points
  if (macd > macdSignal && macd > 0) {
    confidence = Math.min(1, confidence + 0.1);
  }
  
  return confidence;
}

/**
 * Calculate downtrend confidence
 */
function calculateDowntrendConfidence(maDiff, maLongDiff, rsi, momentum, macd, macdSignal, threshold) {
  let confidence = 0;
  
  // Moving average alignment (40%)
  confidence += Math.min(1, Math.abs(maDiff) / (threshold * 4)) * 0.4;
  
  // RSI confirmation (20%)
  const rsiScore = (50 - rsi) / 20; // 30-50 scaled to 1-0
  confidence += Math.max(0, Math.min(1, rsiScore)) * 0.2;
  
  // Momentum confirmation (20%)
  confidence += Math.max(0, Math.min(1, Math.abs(momentum) * 50)) * 0.2;
  
  // Long-term trend alignment (20%)
  confidence += (maLongDiff < 0 ? Math.min(1, Math.abs(maLongDiff) / (threshold * 2)) : 0) * 0.2;
  
  // MACD confirmation - bonus points
  if (macd < macdSignal && macd < 0) {
    confidence = Math.min(1, confidence + 0.1);
  }
  
  return confidence;
}

/**
 * Fetch and process candle data with improved error handling
 * @param {string} symbol - Trading symbol to request candles for
 * @param {Object} options - Optional parameters for the request
 */
function requestCandles(symbol, options = {}) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.warn(`Cannot request candles for ${symbol} - WebSocket not connected`);
    return;
  }

  const requestData = {
    ticks_history: symbol,
    adjust_start_time: 1,
    count: options.count || 50,
    end: "latest",
    granularity: options.granularity || 60, // 5-minute candles by default
    style: options.style || "candles"  // Get OHLC data
  };

  try {
    ws.send(JSON.stringify(requestData));
    
    // Track pending requests for timeout handling
    pendingRequests[symbol] = {
      timestamp: Date.now(),
      retries: 0
    };
    
    // Set timeout for request
    setTimeout(() => {
      if (pendingRequests[symbol] && Date.now() - pendingRequests[symbol].timestamp > 10000) {
        console.warn(`Request timeout for ${symbol}, retrying...`);
        if (pendingRequests[symbol].retries < 3) {
          pendingRequests[symbol].retries++;
          requestCandles(symbol, options);
        } else {
          console.error(`Failed to get data for ${symbol} after 3 retries`);
          delete pendingRequests[symbol];
        }
      }
    }, 10000);
    
  } catch (error) {
    console.error(`Error requesting candles for ${symbol}:`, error);
  }
}

/**
 * Subscribe to real-time ticks for a symbol
 */
function subscribeToCandles(symbol) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    console.warn(`Cannot subscribe to ${symbol} - WebSocket not connected`);
    return;
  }

  try {
    ws.send(JSON.stringify({
      ticks: symbol,
      subscribe: 1
    }));
  } catch (error) {
    console.error(`Error subscribing to ${symbol}:`, error);
  }
}

// Schedule regular data refresh
function scheduleRefresh() {
  setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      FOREX_PAIRS.forEach(symbol => requestCandles(symbol));
    }
  }, 60000); // Refresh every 60 seconds
}

// Start the application
function startApp() {
    initializeWebSocket();
  scheduleRefresh();
  
  // Send a startup notification
  sendTelegramAlert("🤖 Bot Started: Volatility Indices Monitor is now active and analyzing market trends.");
  
  // Add process termination handlers
  process.on('SIGINT', () => {
    console.log('Gracefully shutting down...');
    if (ws) {
      ws.close();
    }
    process.exit(0);
  });
  
  process.on('uncaughtException', (error) => {
    console.error('Uncaught exception:', error);
    // Don't exit, let the reconnection logic handle it
  });
}

// Start the application
startApp();