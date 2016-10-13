package com.microsoft.example;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.FileReader;
import java.util.Properties;

import org.apache.storm.eventhubs.bolt.EventHubBolt;
import org.apache.storm.eventhubs.bolt.EventHubBoltConfig;

import com.microsoft.example.DeviceSpout;

public class EventHubWriter {
  //Entry point for the topology
  public static void main(String[] args) throws Exception {
    //Read and set configuration
    Properties properties = new Properties();
    //Arguments? Or from config file?
    if(args.length > 1) {
      properties.load(new FileReader(args[1]));
    }
    else {
      properties.load(EventHubWriter.class.getClassLoader().getResourceAsStream(
        "EventHubs.properties"));
    }
    //Configure the bolt for Event Hub
    String policyName = properties.getProperty("eventhubs.writerpolicyname");
    String policyKey = properties.getProperty("eventhubs.writerpolicykey");
    String namespaceName = properties.getProperty("eventhubs.namespace");
    String entityPath = properties.getProperty("eventhubs.entitypath");

    EventHubBoltConfig spoutConfig = new EventHubBoltConfig(policyName, policyKey,
      namespaceName, "servicebus.windows.net", entityPath);

    //Used to build the topology
    TopologyBuilder builder = new TopologyBuilder();
    //Add the spout, with a name of 'spout'
    //and parallelism hint of 5 executors
    builder.setSpout("spout", new DeviceSpout(), 5);

    builder.setBolt("eventhubbolt", new EventHubBolt(spoutConfig), 8).shuffleGrouping("spout");

    //new configuration
    Config conf = new Config();
    conf.setDebug(true);

    //If there are arguments, we are running on a cluster
    if (args != null && args.length > 0) {
      //parallelism hint to set the number of workers
      conf.setNumWorkers(3);
      //submit the topology
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    //Otherwise, we are running locally
    else {
      //Cap the maximum number of executors that can be spawned
      //for a component to 3
      conf.setMaxTaskParallelism(3);
      //LocalCluster is used to run locally
      LocalCluster cluster = new LocalCluster();
      //submit the topology
      cluster.submitTopology("writer", conf, builder.createTopology());
      //sleep
      Thread.sleep(10000);
      //shut down the cluster
      cluster.shutdown();
    }
  }
}
