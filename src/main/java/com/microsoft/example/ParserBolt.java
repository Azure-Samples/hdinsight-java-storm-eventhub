package com.microsoft.example;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.json.JSONObject;

// import com.google.gson.Gson;
// import com.google.gson.GsonBuilder;

public class ParserBolt extends BaseBasicBolt {

  //Declare output fields & streams
  //hbasestream is all fields, and goes to hbase
  //dashstream is just the device and temperature, and goes to the dashboard
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("deviceId", "deviceValue"));
  }

  //Process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //Should only be one tuple, which is the JSON message from the spout
    String value = tuple.getString(0);

    //Deal with cases where we get multiple
    //EventHub messages in one tuple
    String[] arr = value.split("}");
    for (String ehm : arr)
    {
      //Convert it from JSON to an object
      JSONObject msg=new JSONObject(ehm.concat("}"));
      //Pull out the values and emit to the stream
      String deviceid = msg.getString("deviceId");
      int devicevalue = msg.getInt("deviceValue");
      collector.emit(new Values(deviceid, devicevalue));
    }
  }
}
