
package speed_layer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;


/**
 * Receives tweets and splits its words. Only emits a pair (word, text) if the word is one of the keyword
 * chosen at the start of the computation
 */
public class TweetWordSplitterBolt extends BaseRichBolt {

    private final List<String> keywords;

    private OutputCollector collector;

    public TweetWordSplitterBolt(List<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String text = (String) input.getValueByField("tweet_text");
        String[] words = text.toLowerCase().split(" ");
        for (String word : words) {
            if(keywords.contains(word)){
                //System.out.println(word + "<-keyword , text of the tweet->"+tweetID);

                //Are emitted only the tuple of tweets that contains at least a keyword. If the tweet contains n
                //keywords, n tuple are emitted, one for each keyword.
                collector.emit(new Values(word, text));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "text"));
    }
}