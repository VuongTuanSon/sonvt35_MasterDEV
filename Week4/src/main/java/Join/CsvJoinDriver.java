package Join ;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class CsvJoinDriver {
    public static void main(String[] a) throws ClassNotFoundException, IOException, InterruptedException{
        Configuration config = new Configuration();

        Job job = Job.getInstance(config);
        job.setJarByClass(CsvJoinDriver.class);
        job.setMapperClass(CsvJoinMapper.class);
        job.setReducerClass(CsvJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CsvWritable.class);

        FileInputFormat.addInputPath(job, new Path("/user/sonvt35/input1"));
        FileInputFormat.addInputPath(job, new Path("/user/sonvt35/input2"));
        FileOutputFormat.setOutputPath(job, new Path("/user/sonvt35/output3"));
        job.waitForCompletion(true);
    }
}
