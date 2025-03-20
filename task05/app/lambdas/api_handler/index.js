import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "uuid";

const dbClient = new DynamoDBClient();
const TABLE_NAME = process.env.TABLE_NAME || "Events";

export const handler = async (event) => {
    console.log("Incoming event:", JSON.stringify(event, null, 2));
    
    let requestBody;
    try {
        requestBody = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
    } catch (error) {
        console.error("Failed to parse request body:", error);
        return {
            statusCode: 400,
            body: JSON.stringify({ message: "Malformed JSON in request body" })
        };
    }

    const { principalId, content } = requestBody || {};
    if (!principalId || content === undefined) {
        console.error("Validation error: Missing required parameters", requestBody);
        return {
            statusCode: 400,
            body: JSON.stringify({ message: "Invalid input: 'principalId' and 'content' are required" })
        };
    }

    const newEvent = {
        id: uuidv4(),
        principalId: Number(principalId),
        createdAt: new Date().toISOString(),
        body: content
    };

    console.log("Storing item in DynamoDB:", JSON.stringify(newEvent, null, 2));
    
    try {
        const dbResponse = await dbClient.send(new PutCommand({
            TableName: TABLE_NAME,
            Item: newEvent
        }));
        console.log("DynamoDB insert successful", dbResponse);
        
        return {
            statusCode: 201,
            body: JSON.stringify({ statusCode: 201, event: newEvent })
        };
    } catch (error) {
        console.error("DynamoDB operation failed:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal server error", error: error.message })
        };
    }
};
