
package speed_layer;

import my_utilities.DatabaseManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import javax.xml.crypto.Data;
import java.util.Map;

/**
 * Receives as input the pair ("keyword", "sentiment") and counts how many pairs arrive of that type. It then updates
 * the realtime view
 */
public class TweetCountSentimentBolt extends BaseRichBolt {
    Table realtimeView;
    DatabaseManager databaseManager;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.databaseManager=DatabaseManager.getInstance();
        this.realtimeView=databaseManager.getRealtimeView();
    }

    @Override
    public void execute(Tuple input) {
        String keyword = (String) input.getValueByField("keyword");
        String sentiment = (String) input.getValueByField("sentiment");
        if(sentiment.equals("0"))
            sentiment="negative_tweets";
        else
            sentiment="positive_tweets";

        databaseManager.incrementRealtimeView(databaseManager.getRealtimeView(),keyword,sentiment);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //no output in terms of pairs from this bolt
    }
}
