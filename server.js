const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

const PORT = 3001;
const MQTT_BROKER = 'mqtt://192.168.1.12';
const MQTT_TOPIC = '#suhu #kelembaban #cahaya #gas #api #gerak #jarak #getaran #tekanan #ketinggian #kecepatan #arah_angin #curah_hujan';

app.use(express.static('public'));

let allTopicsData = new Map();
let topicsList = [];
let messageCount = 0;
const MAX_DATA_POINTS_PER_TOPIC = 100;

const dataFile = path.join(__dirname, 'logs', 'all_sensors_data.json');
if (fs.existsSync(dataFile)) {
    try {
        const rawData = fs.readFileSync(dataFile);
        const savedData = JSON.parse(rawData);
        allTopicsData = new Map(Object.entries(savedData));
        topicsList = Array.from(allTopicsData.keys());
        console.log(`Loaded ${topicsList.length} topics from file`);
    } catch (e) {
        console.log('No existing data file');
    }
}

setInterval(() => {
    try {
        const dataToSave = Object.fromEntries(allTopicsData);
        fs.writeFileSync(dataFile, JSON.stringify(dataToSave, null, 2));
    } catch (e) {
        console.error('Error saving data:', e);
    }
}, 10000);

const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: 'adrian',
    password: 'kaguya123'
});

mqttClient.on('connect', () => {
    console.log(`Connected to MQTT broker: ${MQTT_BROKER}`);
    mqttClient.subscribe(MQTT_TOPIC, (err) => {
        if (!err) console.log(`Subscribed to all topics (#)`);
    });
});

mqttClient.on('message', (topic, message) => {
    try {
        let data;
        const messageStr = message.toString();
        try {
            data = JSON.parse(messageStr);
        } catch (e) {
            data = { raw_message: messageStr, timestamp: Math.floor(Date.now() / 1000) };
        }

        if (!data.timestamp) data.timestamp = Math.floor(Date.now() / 1000);

        const enrichedData = {
            ...data,
            topic,
            timestamp_readable: new Date(data.timestamp * 1000).toLocaleString('id-ID'),
            received_at: new Date().toLocaleString('id-ID')
        };

        if (!allTopicsData.has(topic)) {
            allTopicsData.set(topic, []);
            topicsList.push(topic);
            console.log(`New topic detected: ${topic}`);
        }

        const topicData = allTopicsData.get(topic);
        topicData.push(enrichedData);
        if (topicData.length > MAX_DATA_POINTS_PER_TOPIC) topicData.shift();

        messageCount++;

        io.emit('sensor-data', {
            topic,
            data: enrichedData,
            allTopics: Object.fromEntries(allTopicsData),
            topicsList,
            messageCount
        });

        console.log(`[${new Date().toLocaleTimeString()}] Topic: ${topic} | Data: ${messageStr.substring(0, 100)}`);
    } catch (e) {
        console.error('Error parsing MQTT message:', e);
    }
});

mqttClient.on('error', (err) => console.error('MQTT Error:', err));

app.get('/api/data', (req, res) => {
    const { topic } = req.query;
    if (topic && allTopicsData.has(topic)) {
        res.json(allTopicsData.get(topic));
    } else {
        res.json(Object.fromEntries(allTopicsData));
    }
});

app.get('/api/topics', (req, res) => res.json(topicsList));

app.get('/api/latest', (req, res) => {
    const latest = {};
    for (const [topic, data] of allTopicsData) {
        if (data.length > 0) latest[topic] = data[data.length - 1];
    }
    res.json(latest);
});

app.get('/api/stats', (req, res) => {
    const stats = {};
    for (const [topic, data] of allTopicsData) {
        if (data.length > 0) {
            const numericKeys = Object.keys(data[0]).filter(k =>
                !['topic','timestamp','timestamp_readable','received_at','raw_message'].includes(k) &&
                typeof data[0][k] === 'number'
            );
            stats[topic] = {};
            for (const key of numericKeys) {
                const vals = data.map(d => d[key]).filter(v => v !== undefined);
                stats[topic][key] = {
                    avg: vals.reduce((a, b) => a + b, 0) / vals.length,
                    min: Math.min(...vals),
                    max: Math.max(...vals)
                };
            }
        }
    }
    res.json(stats);
});

server.listen(PORT, () => {
    console.log(`Dashboard running at http://localhost:${PORT}`);
    console.log(`Monitoring all MQTT topics from ${MQTT_BROKER}`);
});
