import express from "express";
import bodyParser from "body-parser";
import { connect as connectRabbitMQ } from "amqplib";
import { v4 as uuidv4 } from "uuid";
import mongoose from "mongoose";

//! MongoDB Connection with Mongoose

async function connectMongoDB() {
  try {
    // await mongoose.connect(mongoUrl);
    console.log("Connected to MongoDB with Mongoose");
  } catch (err) {
    console.error("MongoDB connection error:", err);
  }
}
connectMongoDB();

// Define Mongoose Schema and Model for Claims
const claimSchema = new mongoose.Schema({
  claimId: { type: String, required: true },
  status: { type: String, required: true },
  claimantName: { type: String, required: true },
  claimantEmail: { type: String, required: true },
  claimantPhone: { type: String, required: true },
  incidentDetails: { type: String, required: true },
});
const Claim = mongoose.model("Claim", claimSchema);

// Express Server Setup (API Gateway)
const app = express();
app.use(bodyParser.json());

//! RabbitMQ Setup
const rabbitMQUrl = "amqp://localhost";
let channel;

async function initializeRabbitMQ() {
  try {
    const connection = await connectRabbitMQ(rabbitMQUrl);
    channel = await connection.createChannel();
    await channel.assertQueue("claim_queue");
    console.log("Connected to RabbitMQ");
  } catch (err) {
    console.error("RabbitMQ connection error:", err);
  }
}
initializeRabbitMQ();

// API Gateway Route to Submit Claim
app.post("/submit-claim", async (req, res) => {
  const claimId = uuidv4();
  const claimData = { ...req.body, claimId, status: "Pending" };

  try {
    // Save claim to MongoDB
    // const claim = new Claim(claimData);
    // await claim.save();

    console.log(claimData);

    // Send claim data to RabbitMQ queue
    channel.sendToQueue("claim_queue", Buffer.from(JSON.stringify(claimData)));
    res.status(200).json({ message: "Claim submitted successfully", claimId });
  } catch (err) {
    console.error("Error submitting claim:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Start Server
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`API Gateway running on http://localhost:${PORT}`);
});
