package batch_layer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.BasicConfigurator;


public class BatchDriver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception{
        System.out.println("Starting Map Reduce for Hadoop Batch Layer");

        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("tweets"));

        Job job = Job.getInstance(conf, "BatchSentimentAnalysis");

        job.setJarByClass(BatchDriver.class);

        TableMapReduceUtil.initTableMapperJob("masterDatabase", scan, BatchMap.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("batchViewInComputation", BatchReducer.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}


