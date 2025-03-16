import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "uuid";

// Create DynamoDB client
const dynamoDBClient = new DynamoDBClient({ region: "eu-west-1" });
const TABLE_NAME = process.env.TARGET_TABLE;

export const handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2)); // Log incoming event for debugging

    // Step 1: Parse and validate the input
    let inputEvent;

    try {
        inputEvent = typeof event.body === "string" ? JSON.parse(event.body) : event.body; // Check if body is a string (from API Gateway)
    } catch (parseError) {
        console.error("Error parsing event body:", parseError); // Log parsing error
        return {
            statusCode: 400,
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ message: "Invalid JSON format in request body" }),
        };
    }

    // Validate required fields
    if (!inputEvent?.principalId || inputEvent?.content === undefined) {
        console.error("Validation failed: Missing required fields", inputEvent); // Log validation error
        return {
            statusCode: 400,
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ message: "Invalid input: principalId and content are required" }),
        };
    }

    // Step 2: Prepare data for DynamoDB
    const eventId = uuidv4();
    const createdAt = new Date().toISOString(); // Create timestamp in ISO 8601 format

    const eventItem = {
        id: eventId, // Primary key
        principalId: Number(inputEvent.principalId), // Cast principalId to a number
        createdAt, // ISO timestamp
        body: inputEvent.content, // Full body content
    };

    console.log("Preparing to save to DynamoDB:", JSON.stringify(eventItem, null, 2)); // Log the item to be saved

    // Step 3: Save the data to DynamoDB
    try {
        await dynamoDBClient.send(
            new PutCommand({
                TableName: TABLE_NAME,
                Item: eventItem, // The event payload
            })
        );
        console.log("Saved successfully to DynamoDB."); // Log success

        // Step 4: Return success response
        return {
            statusCode: 201,
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                message: "Event saved successfully",
                event: eventItem, // Return the saved item in the response
            }),
        };
    } catch (dbError) {
        console.error("DynamoDB put error:", dbError); // Log DynamoDB error
        return {
            statusCode: 500,
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                message: "Failed to save event to DynamoDB",
                error: dbError.message, // Include the error message for debugging
            }),
        };
    }
};

// Global error catch block (not required but useful for unexpected failures)
process.on("unhandledRejection", (reason) => {
    console.error("Unhandled Rejection in Lambda:", reason); // Log unhandled promise rejections
});

process.on("uncaughtException", (error) => {
    console.error("Uncaught Exception in Lambda:", error); // Log uncaught exceptions
});