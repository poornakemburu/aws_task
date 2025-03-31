// const AWS = require("aws-sdk");
// const axios = require("axios");
// const { v4: uuidv4 } = require("uuid");

// const dynamoDB = new AWS.DynamoDB.DocumentClient();
// const TABLE_NAME = process.env.TARGET_TABLE || "Weather";

// async function fetchWeather() {
//     const url = "https://api.open-meteo.com/v1/forecast?latitude=50.4375&longitude=30.5&hourly=temperature_2m";

//     try {
//         const response = await axios.get(url);
//         console.log("Fetched weather data:", JSON.stringify(response.data, null, 2));
//         return response.data;
//     } catch (error) {
//         console.error("Error fetching weather data:", error);
//         throw new Error("Failed to fetch weather data");
//     }
// }

// exports.handler = async (event) => {
//     try {
//         console.log("Received event:", JSON.stringify(event, null, 2));

//         const weatherData = await fetchWeather();

//         const item = {
//             id: uuidv4(),
//             forecast: {
//                 latitude: weatherData.latitude,
//                 longitude: weatherData.longitude,
//                 generationtime_ms: weatherData.generationtime_ms,
//                 utc_offset_seconds: weatherData.utc_offset_seconds,
//                 timezone: weatherData.timezone,
//                 timezone_abbreviation: weatherData.timezone_abbreviation,
//                 elevation: weatherData.elevation,
//                 hourly_units: weatherData.hourly_units,
//                 hourly: weatherData.hourly
//             }
//         };

//         console.log("Saving item to DynamoDB:", JSON.stringify(item, null, 2));

//         await dynamoDB.put({
//             TableName: TABLE_NAME,
//             Item: item
//         }).promise().then(() => {
//             console.log("Successfully inserted item into DynamoDB");
//         }).catch(err => {
//             console.error("DynamoDB put error:", err);
//             throw new Error("Failed to store data in DynamoDB");
//         });

//         return {
//             statusCode: 200,
//             body: JSON.stringify({ message: "Weather data stored successfully!" }),
//             headers: { "Content-Type": "application/json" }
//         };

//     } catch (error) {
//         console.error("Error processing request:", error);
//         return {
//             statusCode: 500,
//             body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
//             headers: { "Content-Type": "application/json" }
//         };
//     }
// };
// Import required dependencies with destructuring where applicable
const { DynamoDB } = require("aws-sdk");
const { default: fetch } = require("node-fetch");
const crypto = require("crypto");

// Create database client with more descriptive naming
const weatherDatabase = new DynamoDB.DocumentClient();
const WEATHER_STORAGE_TABLE = process.env.WEATHER_TABLE_NAME || "WeatherRecords";

// Use a class-based approach to organize functionality
class WeatherService {
  constructor(latitude = 50.4375, longitude = 30.5) {
    this.baseUrl = "https://api.open-meteo.com/v1/forecast";
    this.latitude = latitude;
    this.longitude = longitude;
  }

  // Method to build the API URL with parameters
  buildRequestUrl() {
    const params = new URLSearchParams({
      latitude: this.latitude,
      longitude: this.longitude,
      hourly: "temperature_2m"
    });
    
    return `${this.baseUrl}?${params.toString()}`;
  }

  // Method to retrieve weather data
  async retrieveCurrentForecast() {
    const requestUrl = this.buildRequestUrl();
    
    try {
      const response = await fetch(requestUrl);
      
      if (!response.ok) {
        throw new Error(`Weather API returned ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      this.logInfo("Successfully retrieved weather forecast", data);
      return data;
    } catch (error) {
      this.logError("Weather data retrieval failed", error);
      throw new Error(`Weather service error: ${error.message}`);
    }
  }

  // Helper methods for consistent logging
  logInfo(message, data = {}) {
    console.log(`INFO: ${message}`, JSON.stringify(data, null, 2));
  }
  
  logError(message, error) {
    console.error(`ERROR: ${message}`, error);
  }
}

// Database operations in a separate class
class WeatherRepository {
  constructor(tableName) {
    this.tableName = tableName;
    this.dbClient = weatherDatabase;
  }
  
  // Generate a unique ID using custom approach instead of uuid
  generateUniqueId() {
    return crypto.randomBytes(16).toString('hex');
  }
  
  // Transform raw API data to our database schema
  transformWeatherData(rawData) {
    return {
      recordId: this.generateUniqueId(),
      timestamp: new Date().toISOString(),
      location: {
        lat: rawData.latitude,
        lon: rawData.longitude,
        elevation: rawData.elevation
      },
      metadata: {
        generationTime: rawData.generationtime_ms,
        utcOffset: rawData.utc_offset_seconds,
        timezone: rawData.timezone,
        timezoneAbbr: rawData.timezone_abbreviation
      },
      measurements: {
        units: rawData.hourly_units,
        readings: rawData.hourly
      }
    };
  }
  
  // Save transformed data to database
  async saveWeatherRecord(weatherData) {
    const transformedData = this.transformWeatherData(weatherData);
    
    try {
      await this.dbClient.put({
        TableName: this.tableName,
        Item: transformedData
      }).promise();
      
      console.log("Weather record saved successfully", { recordId: transformedData.recordId });
      return transformedData.recordId;
    } catch (dbError) {
      console.error("Database operation failed", dbError);
      throw new Error(`Database error: ${dbError.message}`);
    }
  }
}

// Lambda handler using the new class structure
exports.handler = async (event) => {
  // Initialize our services
  const weatherService = new WeatherService();
  const repository = new WeatherRepository(WEATHER_STORAGE_TABLE);
  
  try {
    console.log("Lambda invocation started", { event: JSON.stringify(event) });
    
    // Get weather data
    const forecastData = await weatherService.retrieveCurrentForecast();
    
    // Save to database
    const recordId = await repository.saveWeatherRecord(forecastData);
    
    // Return success response
    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        success: true,
        message: "Weather forecast processed and stored",
        recordId
      })
    };
    
  } catch (error) {
    console.error("Lambda execution failed", error);
    
    // Return error response
    return {
      statusCode: error.statusCode || 500,
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        success: false,
        error: error.message || "Unknown error occurred",
        timestamp: new Date().toISOString()
      })
    };
  }
};