package batch_layer;

import my_utilities.DatabaseManager;
import my_utilities.SentimentTweetClassifier;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class BatchMap extends TableMapper<Text, Text>{

    private static List<String> keywords;
    private static SentimentTweetClassifier classifier;
    private static DatabaseManager databaseManager;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String[] array = new String[]{"apple", "microsoft", "google", "amazon"};
        // Convert String Array to List
        keywords = Arrays.asList(array);
        classifier = new SentimentTweetClassifier();
        databaseManager = DatabaseManager.getInstance();
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] byteText = value.getValue(Bytes.toBytes("tweets"), Bytes.toBytes("text"));
        String line = new String(byteText);

        //Splitting the words of the tweet_text
        String[] tweet_text = line.split(" ");

        for (String word : tweet_text) {
            //If the tweet text contains at least a keyword, then...
            if (keywords.contains(word)) {
                //is computed the sentiment for that tweet and that keywords.
                String sentiment = classifier.classify(line);
                //is emitted the tuple (keyword, sentiment)
                context.write(new Text(word), new Text(sentiment));
            }
        }
    }
}
