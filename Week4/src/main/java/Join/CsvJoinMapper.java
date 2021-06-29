package Join;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CsvJoinMapper extends Mapper<LongWritable, Text, Text, CsvWritable>{
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, CsvWritable>.Context context)  throws IOException, InterruptedException {
        String data = value.toString();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (fileName.contains("people")) {
            String[] dataArray = data.split(",");
            String ID = dataArray[0];
            String firstName = dataArray[1];
            String lastName = dataArray[2];
            String email = dataArray[3];
            String city = dataArray[4];
            String profession = dataArray[5];
            String fieldName = dataArray[6];

            CsvWritable csv = new CsvWritable();
            csv.setID(new Text(ID));
            csv.setFirstName(new Text(firstName));
            csv.setLastName(new Text(lastName));
            csv.setEmail(new Text(email));
            csv.setCity(new Text(city));
            csv.setProfession(new Text(profession));
            csv.setFieldName(new Text(fieldName));
            context.write(csv.getProfession(), csv);
        } else if (fileName.contains("salary")){
            String[] dataArray = data.split(",");

            String profession = dataArray[0];
            String salary = dataArray[1];

            CsvWritable csv = new CsvWritable();
            csv.setProfession(new Text(profession));
            csv.setSalary(new Text(salary));
            context.write(csv.getProfession(), csv);
        }
    }
    }

