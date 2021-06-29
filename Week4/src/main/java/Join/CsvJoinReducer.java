package Join;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CsvJoinReducer extends Reducer<Text, CsvWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<CsvWritable> values, Reducer<Text, CsvWritable, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Iterator<CsvWritable> it = values.iterator();
        String ID = "";
        String firstName = "";
        String lastName = "";
        String email = "";
        String city = "";
        String profession = "";
        String fieldName = "";
        String salary = "";
        String out = "";

        while(it.hasNext()){
            CsvWritable csv = it.next();
            if (csv.getSalary().toString() != "")
            {
                salary = csv.getSalary().toString();
                break;
            }
        }
        while(it.hasNext()){
            CsvWritable csv = it.next();
            ID = csv.getID().toString();
            firstName = csv.getFirstName().toString();
            lastName = csv.getLastName().toString();
            email = csv.getEmail().toString();
            city = csv.getCity().toString();
            profession = csv.getProfession().toString();
            fieldName = csv.getFieldName().toString();

            if (ID != "" && salary != "" && firstName != "" && lastName != "" && email != "" && city != "" && profession != "" && fieldName != "") {
                out += ID + "," + firstName + "," + lastName + "," + email + "," + city + "," + profession + "," + fieldName
                        + "," + salary + "\n";
            }
        }
        context.write(null, new Text(out));

    }
}

