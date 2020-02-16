
package batch_layer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Text;
import java.io.IOException;


public class BatchReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        long positive_count=0L;
        long negative_count=0L;
        for(Text value: values){
            if(value.equals(new Text("1"))){
                positive_count++;
            }
            else{
                negative_count++;
            }
        }
        Put updatedRow=new Put(Bytes.toBytes(key.toString()));

        updatedRow.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("positive_tweets"), Bytes.toBytes(positive_count));
        updatedRow.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("negative_tweets"), Bytes.toBytes(negative_count));

        context.write(null, updatedRow);
    }
}
