import my_utilities.DatabaseManager;

import java.util.List;

public class ServingLayer implements Runnable {
    private List<String> keywords;
    private DatabaseManager databaseManager = DatabaseManager.getInstance();
    private Executor executor;

    public ServingLayer(List<String>keywords, Executor executor) {
        this.keywords = keywords;
        this.executor=executor;
    }

    @Override
    public void run() {
        try{
            //Sleep in order to give time to fill a bit the tables
            Thread.sleep(40000);
        }catch (Exception e){
            System.out.println("Sleep interrupted");
        }
        //Computing a batch View to fill at least two batch views before printing out the outputs.
        //executor.computeBatchView();

        int k=0;
        while (true) {
            try{
                //Sleep in order to give time to fill a bit the tables
                Thread.sleep(20000);
            }catch (Exception e){
                System.out.println("Sleep interrupted");
            }
            System.out.println("Iter " + k);
            System.out.println("Recomputing batch view");
            executor.computeBatchView();
            try{
                //Sleep in order to give time to finish computation of batch view
                Thread.sleep(5000);
            }catch (Exception e){
                System.out.println("Sleep interrupted");
            }

            //For the complete computation is used the complete batch view, the other one is still computing probably
            System.out.println("Complete View");
            System.out.println("-------------------------------\n");
            for (int i = 0; i < keywords.size(); i++) {
                String sentiment="negative_tweets";
                long count_realtime = databaseManager.selectCountFromKey(databaseManager.getRealtimeView(), keywords.get(i), sentiment);
                long count_batch= databaseManager.selectCountFromKey(databaseManager.getCompleteBatchView(), keywords.get(i), sentiment);
                long total=count_batch+count_realtime;
                System.out.println(keywords.get(i) + " | " + sentiment + " | " + total);

                sentiment="positive_tweets";
                count_realtime = databaseManager.selectCountFromKey(databaseManager.getRealtimeView(), keywords.get(i), sentiment);
                count_batch= databaseManager.selectCountFromKey(databaseManager.getCompleteBatchView(), keywords.get(i), sentiment);
                total=count_batch+count_realtime;
                System.out.println(keywords.get(i) + " | " + sentiment + " | " + total);
            }
            System.out.println("End iter");
        }
    }
}
