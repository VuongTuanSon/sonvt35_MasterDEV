package WordCount;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
public class WC_Mapper extends Mapper <LongWritable, Text, Text, IntWritable> {
    private Text wordToken = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // clean up the text of the line by removing...
        line = line.replaceAll("&.*?\\w+;", " ")               // HTML entities...
                .replaceAll("[^a-zA-Z0-9 ]", " ")         // punctuation...
                .replaceAll("\\s+", " ");               // and getting rid of double spaces


        // if the line has remaining words after the cleanup...
        if (line != null && !line.trim().isEmpty()) {
            String[] words = line.split(" ");   // split the text to words

            // set each word as key to the key-value pair
            for (String word : words)
                context.write(new Text(word + " ,"), new IntWritable(1));
        }
    }
}
