// Import required dependencies
const { DynamoDB } = require("aws-sdk");
const { default: fetch } = require("node-fetch");
const crypto = require("crypto");

// Create database client
const weatherDatabase = new DynamoDB.DocumentClient();
const WEATHER_STORAGE_TABLE = process.env.WEATHER_TABLE_NAME || "Weather";

// Weather service class
class WeatherService {
  constructor(latitude = 50.4375, longitude = 30.5) {
    this.baseUrl = "https://api.open-meteo.com/v1/forecast";
    this.latitude = latitude;
    this.longitude = longitude;
  }

  // Method to build the API URL with EXACT expected parameters
  buildRequestUrl() {
    // IMPORTANT: Only request temperature_2m to match expected output
    // Do not include wind_speed_10m or relative_humidity_2m
    const params = new URLSearchParams({
      latitude: this.latitude,
      longitude: this.longitude,
      hourly: "temperature_2m" // Only request temperature_2m
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

// Database operations
class WeatherRepository {
  constructor(tableName) {
    this.tableName = tableName;
    this.dbClient = weatherDatabase;
  }
  
  // Generate a unique ID
  generateUniqueId() {
    return crypto.randomBytes(16).toString('hex');
  }
  
  // Transform data to match EXACTLY what's expected in DynamoDB
  transformWeatherData(rawData) {
    return {
      id: this.generateUniqueId(),
      forecast: {
        latitude: rawData.latitude,
        longitude: rawData.longitude,
        generationtime_ms: rawData.generationtime_ms,
        utc_offset_seconds: rawData.utc_offset_seconds,
        timezone: rawData.timezone,
        timezone_abbreviation: rawData.timezone_abbreviation,
        elevation: rawData.elevation,
        // IMPORTANT: Keep the exact structure expected for hourly_units
        hourly_units: {
          time: "iso8601",
          temperature_2m: "°C"
        },
        // IMPORTANT: Keep the exact structure expected for hourly
        hourly: {
          time: rawData.hourly.time,
          temperature_2m: rawData.hourly.temperature_2m
        }
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
      
      console.log("Weather record saved successfully", { recordId: transformedData.id });
      return transformedData.id;
    } catch (dbError) {
      console.error("Database operation failed", dbError);
      throw new Error(`Database error: ${dbError.message}`);
    }
  }
}

// Lambda handler
exports.handler = async (event) => {
  // Initialize services
  const weatherService = new WeatherService();
  const repository = new WeatherRepository(WEATHER_STORAGE_TABLE);
  
  try {
    console.log("Lambda invocation started", { event: JSON.stringify(event) });
    
    // Get weather data
    const forecastData = await weatherService.retrieveCurrentForecast();
    
    // Save to database with the exact structure expected
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