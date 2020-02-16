package speed_layer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import my_utilities.*;


/**
 * Receives from TweetWordSplitterBolt a pair (keyword, text) and computes the sentiment upon that keyword and the
 * corresponding tweet
 */
public class TweetSentimentBolt extends BaseRichBolt {

    private SentimentTweetClassifier classifier;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        classifier=new SentimentTweetClassifier();
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String keyword = (String) input.getValueByField("keyword");
        String text = (String) input.getValueByField("text");

        String sentiment= classifier.classify(text);
        //System.out.println("keyword: "+keyword+", sentiment: "+sentiment+", text: "+text);
        collector.emit(new Values(keyword, sentiment));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "sentiment"));
    }

}