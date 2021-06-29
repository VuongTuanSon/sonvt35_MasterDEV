package Join2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalaryRecord implements Writable {
    public Text profession = new Text();
    public Text salary = new Text();
    public SalaryRecord(){}
    public SalaryRecord (String profession, String salary)
    {
        this.profession.set(profession);
        this.salary.set(salary);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        profession.write(out);
        salary.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        profession.readFields(in);
        salary.readFields(in);
    }
}
