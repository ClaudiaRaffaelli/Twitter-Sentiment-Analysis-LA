
import java.util.Arrays;
import java.util.List;


import org.apache.storm.LocalCluster;


public class Main {
    private static String TOPOLOGY_NAME = "apache-storm-twitter-sentiment-analysis";

    public static void main(String[] args) {

        //set of keywords upon which we are interested in performing the sentiment analysis
        String[] array = new String[]{"apple", "microsoft", "google", "amazon"};
        // Convert String Array to List
        List<String> keywords = Arrays.asList(array);
        try{
            //Initialization of Storm Topology, HBase, Hadoop
            Initializer init=new Initializer(keywords);
            Executor executor=new Executor(init.getDBManager(),keywords);

            // executing the storm cluster
            LocalCluster cluster = init.getCluster();
            cluster.submitTopology(TOPOLOGY_NAME, init.getConfiguration(), init.getTopologyBuilder().createTopology());
            //At this point new tweets are being stored in the master Database. Meanwhile the tweets are passed to Storm
            //into the speed layer to provide the realtime View.

            try{
                //Sleep in order to give time to fill a bit the tables
                Thread.sleep(40000);
            }catch (Exception e){
                System.out.println("Sleep interrupted");
            }

            //Starting serving layer
            Thread servingLayer = new Thread(new ServingLayer(keywords, executor));
            servingLayer.start();

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}