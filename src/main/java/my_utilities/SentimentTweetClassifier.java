package my_utilities;
import java.io.File;
import java.io.IOException;

//Import from LingPipe library
import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;

public class SentimentTweetClassifier {
    String[] categories;
    @SuppressWarnings("rawtypes")
    LMClassifier lmc;

    @SuppressWarnings("rawtypes")
    public SentimentTweetClassifier() {
        //Loading serialized object created by the trainer
        try {
            lmc = (LMClassifier) AbstractExternalizable.readObject(new File("./src/main/java/my_utilities/classifier.txt"));
            categories = lmc.categories();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //It classifies the text at input as positive or negative, using the model created
    public String classify(String text) {
        ConditionalClassification classification = lmc.classify(text);
        String sentiment=classification.bestCategory();
        return sentiment;
    }
}