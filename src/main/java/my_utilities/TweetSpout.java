package my_utilities;


//Import needed for the reading of the CSV containing tweets
import com.opencsv.CSVReader;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.*;

@SuppressWarnings({ "rawtypes", "serial" })
public class TweetSpout extends BaseRichSpout {

    private final String fileName;
    private SpoutOutputCollector _collector;
    private CSVReader reader;
    //linesRead counts the number of lines read (not within a file, but in general)
    private AtomicLong tweetID;
    //filesRead counts the number of files read
    private AtomicLong filesRead;
    private DatabaseManager databaseManager;
    private Table masterDB;


    public TweetSpout(String filename) {
        this.fileName = filename;
        tweetID=new AtomicLong(0);
        filesRead=new AtomicLong(0);
    }

    public void init(){
        try{
            reader = new CSVReader(new FileReader(fileName));
            long filesNum=filesRead.incrementAndGet();
            System.out.println("File read "+filesNum+" times, "+tweetID.get()+" lines read");
            if(tweetID.get() % 200 ==1){
                Time.sleep(10000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    //Called when a task for this component is initialized within a worker on the cluster. It provides the spout
    // with the environment in which the spout executes.
    //Inputs: conf is the Storm configuration for this spout. The object context can be used to get information about
    // this task’s place within the topology. The collector is used to emit tuples from this spout.
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        databaseManager=DatabaseManager.getInstance();
        masterDB=databaseManager.getMasterDB();
        _collector = collector;
        init();
    }

    @Override
    public void nextTuple() {
        try {
            //line contains the line read inside the CSV file
            String[] line = reader.readNext();
            if (line != null) {
                //System.out.println(Arrays.toString(line));

                /*
                For sentiment140.csv:
                String tweet_date=line[2];
                String tweet_text=line[5];
                //Writing the tweet to the Master Database (Hadoop)
                Put p=new Put(Bytes.toBytes(Long.toString(tweetID.incrementAndGet())));
                p.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("date"),Bytes.toBytes(line[2]));
                p.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("user"),Bytes.toBytes(line[4]));
                p.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("text"),Bytes.toBytes(line[5]));
                masterDB.put(p);
                 */

                String tweet_date=line[3];
                String tweet_text=line[4];

                //Writing the tweet to the Master Database (Hadoop)
                Put p=new Put(Bytes.toBytes(Long.toString(tweetID.incrementAndGet())));
                p.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("date"),Bytes.toBytes(line[3]));
                p.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("text"),Bytes.toBytes(line[4]));
                masterDB.put(p);

                //Emitting tuple for speed layer (Storm)
                _collector.emit(new Values(tweet_date,tweet_text.toLowerCase()));
            }

            else{
                //The entire file has been read, we just have to start all over again
                //We get and increment the number of files read by the system
                init();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Storm has determined that the tuple emitted by this spout with that id has been fully processed.
    @Override
    public void ack(Object id) {
    }

    //The tuple emitted by this spout with that id has failed to be fully processed.
    @Override
    public void fail(Object id) {
        System.err.println("The tuple with id "+id+" has failed");
    }


    //Declare the output schema for all the streams of this topology (inherited)
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        try {
            //declaring output fields
            ArrayList<String> f= new ArrayList<String>(Arrays.asList("tweet_date", "tweet_text"));
            declarer.declare(new Fields(f));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

