
import batch_layer.BatchDriver;
import my_utilities.DatabaseManager;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class Executor {

    private DatabaseManager databaseManager;
    private List<String> keywords;

    public Executor(DatabaseManager databaseManager, List<String> keywords){
        this.databaseManager=databaseManager;
        this.keywords=keywords;
    }


    public void computeBatchView(){
        try{
            //The old batch view is saved as the most updated and for the new computation is used the old one

            //Re-initialization of realtime view table
            for(String keyword: keywords){
                Put newRow=new Put(Bytes.toBytes(keyword.toString()));

                newRow.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("negative_tweets"), Bytes.toBytes(0L));
                newRow.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("positive_tweets"), Bytes.toBytes(0L));

                databaseManager.getRealtimeView().put(newRow);
            }
            String [] args={};

            ToolRunner.run(new BatchDriver(), args);
            //saving as most updated the just computed batch view
            databaseManager.saveBatchView();
            //25 second sleep to simulate a long batch computation
            Thread.sleep(25000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
