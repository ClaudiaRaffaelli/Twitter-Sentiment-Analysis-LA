//Storm import:

import my_utilities.DatabaseManager;
import my_utilities.TweetSpout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

//HBase import:
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

//Hadoop import:
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import speed_layer.*;



/**
 * In initialize the Storm topology, databases tables and HDFS filesystem
 **/
public class Initializer {

    private LocalCluster cluster;
    private Config config;
    private TopologyBuilder builder;
    private List<String> keywords;
    private Connection hbaseConnection;
    private Configuration hbaseConfiguration;
    private DatabaseManager databaseManager;
    private FileSystem fileSystem;

    public Initializer(List<String> keywords) throws IOException {

        //set of keywords upon which we are interested in performing the sentiment analysis
        this.keywords=keywords;

        config = new Config();
        config.setMessageTimeoutSecs(120);

        //Creation of the three tables needed in the database: batchView, realtimeView, masterDatabase
        hbaseConfiguration= HBaseConfiguration.create();
        hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration);
        databaseManager=new DatabaseManager(hbaseConfiguration,hbaseConnection);
        databaseManager.createDatabases();
        databaseManager.setInstance(databaseManager);

        fileSystem = configureHDFS();

        builder = new TopologyBuilder();
        //builder.setSpout("TweetSpout", new TweetSpout("./datasets/dataset-prova.csv"));
        builder.setSpout("TweetSpout", new TweetSpout("./datasets/full-corpus.csv"));

        builder.setBolt("TweetWordSplitterBolt", new TweetWordSplitterBolt(this.keywords)).shuffleGrouping("TweetSpout");
        //The train needs to be performed before this line
        builder.setBolt("TweetSentimentBolt",new TweetSentimentBolt()).shuffleGrouping("TweetWordSplitterBolt");
        builder.setBolt("TweetCountSentimentBolt",new TweetCountSentimentBolt()).shuffleGrouping("TweetSentimentBolt");

        //creating the storm cluster
        try{
            cluster = new LocalCluster();
        }catch (Exception e){
            System.out.println("Exception in creation of cluster topology");
            e.printStackTrace();
        }
    }


    //Getters

    public DatabaseManager getDBManager(){
        return databaseManager;
    }

    public List<String> getKeywords(){
        return keywords;
    }

    public LocalCluster getCluster(){
        return cluster;
    }

    public Config getConfiguration(){
        return config;
    }

    public TopologyBuilder getTopologyBuilder(){
        return builder;
    }

    public static FileSystem configureHDFS() {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/user/claus");

            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
            conf.setBoolean("dfs.support.append", true);
            fs = FileSystem.get(conf);

        } catch (IOException e) {
            System.out.println("Unable to configure file system!");
        }catch (Exception e){
            e.printStackTrace();
        }
        return fs;
    }

}

