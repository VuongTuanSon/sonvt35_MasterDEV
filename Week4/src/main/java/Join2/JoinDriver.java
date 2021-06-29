package Join2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class JoinDriver extends Configured implements Tool {
    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super (PeopleSalaryKey.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            PeopleSalaryKey first = (PeopleSalaryKey) a;
            PeopleSalaryKey second = (PeopleSalaryKey) b;

            return first.profession.compareTo(second.profession);
        }
    }
    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator()
        {
            super (PeopleSalaryKey.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            PeopleSalaryKey first = (PeopleSalaryKey) a;
            PeopleSalaryKey second = (PeopleSalaryKey) b;

            return first.compareTo(second);
        }
    }
    public static class PeopleMapper extends Mapper<LongWritable, Text, PeopleSalaryKey, JoinGenericWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] recordFields = value.toString().split(",");
            String profession = recordFields[5];
            String id = recordFields[0];
            String firstName = recordFields[1];
            String lastName = recordFields[2];
            String email = recordFields[3];
            String city = recordFields[4];
            String fieldName = recordFields[6];

            PeopleSalaryKey recordKey = new PeopleSalaryKey(profession, PeopleSalaryKey.PEOPLE_RECORD);
            PeopleRecord record = new PeopleRecord(id, firstName, lastName, email, city, profession, fieldName);
            JoinGenericWritable genericRecord = new JoinGenericWritable(record);
            context.write(recordKey, genericRecord);
        }
    }
    public static class SalaryMapper extends Mapper<LongWritable,
            Text, PeopleSalaryKey, JoinGenericWritable>{
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] recordFields = value.toString().split(",");
            String profession =recordFields[0];
            String salary = recordFields[1];

            PeopleSalaryKey recordKey = new PeopleSalaryKey(profession, PeopleSalaryKey.SALARY_RECORD);
            SalaryRecord record = new SalaryRecord(profession, salary);
            JoinGenericWritable genericRecord = new JoinGenericWritable(record);
            context.write(recordKey, genericRecord);
        }
    }
    public static class JoinReducer extends Reducer<PeopleSalaryKey, JoinGenericWritable, NullWritable, Text> {
        public void reduce(PeopleSalaryKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
            StringBuilder output = new StringBuilder();
            String salary ="";
            for (JoinGenericWritable v : values) {
                Writable record = v.get();
                if (key.recordType.equals(PeopleSalaryKey.SALARY_RECORD)){
                    SalaryRecord sRecord = (SalaryRecord) record;
                    salary = sRecord.salary.toString();
                }
                else {
                    PeopleRecord pRecord = (PeopleRecord) record;
                    output.append(key.profession.toString()).append(", ");
                    output.append(pRecord.ID.toString()).append(", ");
                    output.append(pRecord.firstName.toString()).append(", ");
                    output.append(pRecord.lastName.toString()).append(", ");
                    output.append(pRecord.email.toString()).append(", ");
                    output.append(pRecord.city.toString()).append(", ");
                    output.append(pRecord.fieldName.toString()).append(", ");
                    output.append(salary);
                    output.append("\n");
                }
            }
            output.setLength(output.length()-1);
                context.write(NullWritable.get(), new Text(output.toString()));
        }
    }
    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();

        Job job = Job.getInstance(getConf());
        job.setJarByClass(JoinDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(PeopleSalaryKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, PeopleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, SalaryMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            return 0;
        } else {
            return 1;
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        int res = ToolRunner.run(new JoinDriver(), args);
    }
}
