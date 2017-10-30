package com.microsoft.example;

import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ParserBolt extends BaseBasicBolt {
  
    // Declare output fields & streams
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Declare the fields in the tuple. These fields are accessible by
        // these names when read by bolts.
        declarer.declare(new Fields("temperature", "humidity", "co2level", "timestamp"));
    }

    //Process tuples
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // Should only be one tuple, which is the JSON message from the spout
        String value = tuple.getString(0);

        // Deal with cases where we get multiple
        // EventHub messages in one tuple
        String[] arr = value.split("}");
        for (String ehm : arr)
        {
            //Convert it from JSON to an object
            JSONObject msg=new JSONObject(ehm.concat("}"));
            if(msg.has("temperature") && msg.has("humidity") && msg.has("co2level")) {
                // Pull out the values out of JSON and emit as a tuple
                int temperature = msg.getInt("temperature");
                int humidity = msg.getInt("humidity");
                int co2 = msg.getInt("co2level");
                // Create a timestamp, since there is none in the original data
                TimeZone timeZone = TimeZone.getTimeZone("UTC");
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
                dateFormat.setTimeZone(timeZone);
                // Emit to a tuple with values from JSON and the timestamp
                collector.emit(new Values(temperature, humidity, co2, dateFormat.format(new Date())));
            }
        }
    }
}
