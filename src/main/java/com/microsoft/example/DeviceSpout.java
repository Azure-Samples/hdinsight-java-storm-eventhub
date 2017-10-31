package com.microsoft.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import org.json.JSONObject;

import java.util.Map;
import java.util.Random;

//This spout randomly emits device info
public class DeviceSpout extends BaseRichSpout {
    //Collector used to emit output
    SpoutOutputCollector _collector;
    //Used to generate a random number
    Random _rand;

    //Open is called when an instance of the class is created
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    //Set the instance collector to the one passed in
        _collector = collector;
        //For randomness
        _rand = new Random();
    }

    //Emit data to the stream
    @Override
    public void nextTuple() {
        // Sleep for 1 second so the data rate
        // is slower.
        Utils.sleep(1000);
        // Randomly emit device info
        int temperature = _rand.nextInt(11) + 68; // There is a war over whether it's warm or cool in the house
        int humidity = _rand.nextInt(11) + 35; // 35-45 being the recommended humitity in a house
        int co2 = _rand.nextInt(600); // Indoor CO2 range
        //Create a JSON document to send to Event Hub
        // BECAUSE, I learned the hard way that if you don't
        // put it in a nice format that things can interop with,
        // then you have problems later when someone wants
        // to read it into a C# app that uses a framework that
        // expects JSON.
        JSONObject message = new JSONObject();
        message.put("temperature", temperature);
        message.put("humidity", humidity);
        message.put("co2level", co2);
        //Emit the device
        _collector.emit(new Values(message.toString()));
    }

    //Ack is not implemented
    @Override
    public void ack(Object id) {
    }

    //Fail is not implemented
    @Override
    public void fail(Object id) {
    }

    //Declare the output fields. In this case, an single tuple
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
