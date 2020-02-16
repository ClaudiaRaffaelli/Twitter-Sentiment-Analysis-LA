package my_utilities;
//Import from LingPipe library
import com.aliasi.classify.*;
import com.aliasi.corpus.ObjectHandler;
import com.aliasi.util.AbstractExternalizable;
import com.aliasi.util.Compilable;
import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class SentimentTweetTrain {

    public static void main(String[] args) throws Exception {
        train();

    }

    public static void train() throws Exception {
        CSVReader reader = new CSVReader(new FileReader("./datasets/sentiment140.csv"));

        //line contains the line read inside the CSV file (a tweet)
        String[] line = reader.readNext();

        @SuppressWarnings("rawtypes")
        LMClassifier lmc; //Language model classifier
        String []categories={"0","1"};
        int nGram = 7; // the nGram level, any value between 7 and 12 works

        lmc = DynamicLMClassifier.createNGramProcess(categories, nGram);

        while(line!=null){
            //For the training we just need the text of the tweet and the sentiment provided
            String tweet_texts=line[5];
            String sentiment_class = line[0];

            if (sentiment_class.equals("4")){
                sentiment_class="1";
            }
            Classification classification = new Classification(sentiment_class);
            Classified<String> classified = new Classified<String>(tweet_texts, classification);
            ((ObjectHandler<Classified<String>>) lmc).handle(classified);
            line = reader.readNext();
        }

        //Saving the classifier in text file for further use.
        try {
            AbstractExternalizable.compileTo((Compilable) lmc, new File(
                    "classifier.txt"));//saving serialize object to text file
            System.out.println("Successfully created a model");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
