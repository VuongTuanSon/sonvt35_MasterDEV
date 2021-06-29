package NumCount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Num_Reducer extends Reducer<Text,IntWritable,IntWritable,IntWritable>
{
    private int wordCount;

    @Override
    protected void setup(Context context) {
        wordCount = 0;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        ++wordCount;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(0) , new IntWritable(wordCount));
    }
}
