'use strict';


const util = require('util');
const fs = require('fs');
const mqtt = require('mqtt');
const {InfluxDB, Point} = require('@influxdata/influxdb-client');
var mqttClient;
var writeApi;
var influxurl = 'localhost';
var influxtoken = '';
var influxorg = 'org';
var influxbucket = 'heatingsystem';
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
            writeApi.writePoint(convertedMessage);
            writeApi.flush();
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

    return new Point('otgw').tag('measure', fieldName).floatField(fieldName, value).timestamp(new Date());
}

function readConfiguration() {
    var config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
    console.log(util.format('Loaded config: %s', JSON.stringify(config)));
    influxurl = config.influx.url;
    influxtoken = config.influx.token;
    influxorg = config.influx.org;
    influxbucket = config.influx.bucket;
    mqtthost = config.mqtt.host;
    mqttport = config.mqtt.port;
    topics = config.mqtt.topics;
    uniqueID = config.mqtt.uniqueID;
}


function start() {
    readConfiguration();

    writeApi = new InfluxDB({url:influxurl, token:influxtoken}).getWriteApi(influxorg, influxbucket, 'ns')
    writeApi.useDefaultTags({uniqueID: uniqueID})

    mqttClient = mqtt.connect('mqtt://' + mqtthost + ':' + mqttport);

    connectAndSubscribe();
    listenForMessages();
}

start();
