'use strict';


const util = require('util');
const fs = require('fs');
const mqtt = require('mqtt');
const Influx = require('influx');
var mqttClient;
var influx;
var influxhost = 'localhost';
var influxport = 8086;
var databasename = 'heatingsystem';
var mqtthost = 'localhost';
var mqttport = 1883;
var uniqueID;
var topics = [];

function connectAndSubscribe() {
    mqttClient.on('connect', function () {
        topics.forEach(function(topic) {
            mqttClient.subscribe(topic.replace("{uniqueID}", uniqueID));
        });
    });
}

function listenForMessages() {
    mqttClient.on('message', function (topic, message) {
        var convertedMessage = convertToInfluxPoint(topic, message);
        try {
            influx.writePoints([convertedMessage]);
        } catch (err) {
            console.error("Something went wrong writing to influxdb", err);
        }
    });
}

function getMeasurementNameFromTopic(topic) {
    var parts = topic.split("/");
    return parts[parts.length - 1];
}

function extractValue(message) {
    if (message == "OFF") {
        return 0;
    } else if (message == "ON") {
        return 1;
    }
    return parseFloat(message);
}

function convertToInfluxPoint(topic, message) {
    var fieldName = getMeasurementNameFromTopic(topic);
    var value = extractValue(message);
    var fields = {};
    fields[fieldName] = value;
    return {
        measurement: 'otgw',
        tags: { measure: fieldName, uniqueID: uniqueID},
        fields : fields,
        "timestamp" : new Date()
    };
}

function readConfiguration() {
    var config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
    console.log(util.format('Loaded config: %s', JSON.stringify(config)));
    influxhost = config.influx.host;
    influxport = config.influx.port;
    mqtthost = config.mqtt.host;
    mqttport = config.mqtt.port;
    topics = config.mqtt.topics;
    uniqueID = config.mqtt.uniqueID;
}


function start() {
    readConfiguration();

    influx = new Influx.InfluxDB({
        host: influxhost,
        port: influxport,
        database: databasename
    });

    influx.createDatabase('heatingsystem');

    mqttClient = mqtt.connect('mqtt://' + mqtthost + ':' + mqttport);

    connectAndSubscribe();
    listenForMessages();
}

start();
