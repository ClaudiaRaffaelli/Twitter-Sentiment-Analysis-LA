package my_utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.util.Arrays;
import java.util.List;


public class DatabaseManager {

    private Connection hbaseConnection;
    private Configuration hbaseConfiguration;
    private Admin admin;

    //BatchView and realtimeView provides the columns: keyword (key), sentiment (negative or positive) and count
    private Table masterDB;
    private Table batchViewComplete;
    private Table batchViewInComputation;
    private Table realtimeView;

    private List<String> keywords;
    private static DatabaseManager instance;

    public DatabaseManager(Configuration hbaseConfiguration,Connection hbaseConnection){
        try{
            this.hbaseConfiguration= hbaseConfiguration;
            this.hbaseConnection = hbaseConnection;
            admin = hbaseConnection.getAdmin();

            //set of keywords upon which we are interested in performing the sentiment analysis
            String[] array = new String[]{"apple", "microsoft", "google", "amazon"};
            // Convert String Array to List
            this.keywords = Arrays.asList(array);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    //Returns the current instance already initialized of the DatabaseManager
    public static DatabaseManager getInstance(){
        return instance;
    }

    public void setInstance(DatabaseManager instance){
        this.instance=instance;
    }

    public void createDatabases(){
        try{
            //Deleting old tables if present
            if (admin.tableExists(TableName.valueOf("masterDatabase"))) {
                admin.disableTable(TableName.valueOf("masterDatabase"));
                admin.deleteTable(TableName.valueOf("masterDatabase"));
            }

            //Creation of masterDB table: the column tweets has columns Date and tweet_text (it also has a timestamp)
            TableDescriptor tweetmasterDB = TableDescriptorBuilder.newBuilder(TableName.valueOf("masterDatabase"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("tweets".getBytes()).build())
                    .build();
            admin.createTable(tweetmasterDB);

            initializeRealtimeViewTable();
            initializeBatchViewComplete();
            initializeBatchViewInComputation();

            masterDB = hbaseConnection.getTable(TableName.valueOf("masterDatabase"));
        }catch(Exception e ){
            e.printStackTrace();
        }
    }

    public void initializeBatchViewComplete(){
        try{

            //Deleting old tables if present
            if (admin.tableExists(TableName.valueOf("batchViewComplete"))) {
                admin.disableTable(TableName.valueOf("batchViewComplete"));
                admin.deleteTable(TableName.valueOf("batchViewComplete"));
            }

            //Creation of batch view table complete
            TableDescriptor tweetBatchViewComplete = TableDescriptorBuilder.newBuilder(TableName.valueOf("batchViewComplete"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("sentiment_analysis".getBytes()).build())
                    .build();
            admin.createTable(tweetBatchViewComplete);

            //Getting the table:
            batchViewComplete = hbaseConnection.getTable(TableName.valueOf("batchViewComplete"));

            for (String key:keywords) {
                Put p = new Put(Bytes.toBytes(key));

                p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("negative_tweets"), Bytes.toBytes(0L));
                p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("positive_tweets"), Bytes.toBytes(0L));
                batchViewComplete.put(p);
            }
        }catch(Exception e ){
            e.printStackTrace();
        }
    }

    public void initializeBatchViewInComputation(){
        try{

            //Deleting old tables if present
            if (admin.tableExists(TableName.valueOf("batchViewInComputation"))) {
                admin.disableTable(TableName.valueOf("batchViewInComputation"));
                admin.deleteTable(TableName.valueOf("batchViewInComputation"));
            }

            //Creation of batch view table in computation
            TableDescriptor tweetBatchViewInComputation = TableDescriptorBuilder.newBuilder(TableName.valueOf("batchViewInComputation"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("sentiment_analysis".getBytes()).build())
                    .build();
            admin.createTable(tweetBatchViewInComputation);

            //Getting the table:
            batchViewInComputation = hbaseConnection.getTable(TableName.valueOf("batchViewInComputation"));

            for (String key:keywords) {
                Put p = new Put(Bytes.toBytes(key));

                p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("negative_tweets"), Bytes.toBytes(0L));
                p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("positive_tweets"), Bytes.toBytes(0L));
                batchViewInComputation.put(p);
            }
        }catch(Exception e ){
            e.printStackTrace();
        }
    }


    public void initializeRealtimeViewTable(){
        try{
            //Deleting old tables if present
            if (admin.tableExists(TableName.valueOf("realtimeView"))) {
                admin.disableTable(TableName.valueOf("realtimeView"));
                admin.deleteTable(TableName.valueOf("realtimeView"));
            }

            //Creation of batch view table
            TableDescriptor tweetRealtimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("realtimeView"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("sentiment_analysis".getBytes()).build())
                    .build();
            admin.createTable(tweetRealtimeView);

            //Getting the table:
            realtimeView = hbaseConnection.getTable(TableName.valueOf("realtimeView"));

            for (String key:keywords) {
                Put p = new Put(Bytes.toBytes(key));
                p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("negative_tweets"), Bytes.toBytes(0L));
                p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("positive_tweets"), Bytes.toBytes(0L));

                realtimeView.put(p);
            }
        }catch(Exception e ){
            e.printStackTrace();
        }
    }

    //method used to update the realtime view
    public void incrementRealtimeView(Table table, String keyword, String sentiment){
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes(sentiment));
        s.setCacheBlocks(false);
        try{
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                byte[] key = r.getRow();

                if(Bytes.toString(key).equals(keyword)){

                    //Creating an increment object
                    Increment incr = new Increment(key);
                    incr.setDurability(Durability.SKIP_WAL);

                    // Updating a cell value
                    //sentiment_count is the column family, counter the column and the last paramater the new value (in long)
                    incr.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes(sentiment), 1L);

                    // Saving the increment to the table
                    table.increment(incr);
                    //System.out.println("data Updated");

                }
            }
            rs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public long selectCountFromKey(Table table, String keyword, String sentiment){
        long count=0;
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes(sentiment));
        s.setCacheBlocks(false);
        try{
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                byte[] key = r.getRow();
                if(Bytes.toString(key).equals(keyword)){
                    count= Bytes.toLong(r.getValue(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes(sentiment)));
                }
            }
            rs.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return count;
    }


    //Copying batchViewInComputation to batchViewComplete, after performing a new batch view computation
    public void saveBatchView(){

        Scan s = new Scan();
        String[]sentiments={"positive_tweets","negative_tweets"};
        for(String sentiment: sentiments) {
            s.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes(sentiment));
            s.setCacheBlocks(false);
            try {
                ResultScanner rs = batchViewInComputation.getScanner(s);
                for (Result r : rs) {
                    byte[] key = r.getRow();
                    byte[] value = r.getValue(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes(sentiment));

                    Put p = new Put(key);
                    p.addColumn(Bytes.toBytes("sentiment_analysis"), Bytes.toBytes("negative_tweets"), value);
                    batchViewComplete.put(p);
                }
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //Getters

    public Table getRealtimeView(){
        return realtimeView;
    }
    public Table getMasterDB(){
        return masterDB;
    }

    public Table getCompleteBatchView(){
        return batchViewComplete;
    }
}
