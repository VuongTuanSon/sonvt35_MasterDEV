package Join2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PeopleSalaryKey implements WritableComparable<PeopleSalaryKey> {
    public Text profession = new Text();
    public IntWritable recordType = new IntWritable();
    public PeopleSalaryKey(){}
    public PeopleSalaryKey(String profession, IntWritable recordType){
        this.profession.set(profession);
        this.recordType = recordType;
    }
    @Override
    public int compareTo(PeopleSalaryKey other) {
        if (this.profession.equals(other.profession )) {
            return this.recordType.compareTo(other.recordType);
        } else {
            return this.profession.compareTo(other.profession);
        }
    }
    public boolean equals (PeopleSalaryKey other) {
        return this.profession.equals(other.profession) && this.recordType.equals(other.recordType );
    }

    public int hashCode() {
        return this.profession.hashCode();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        this.profession.write(out);
        this.recordType.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.profession.readFields(in);
        this.recordType.readFields(in);
    }
    public static final IntWritable PEOPLE_RECORD = new IntWritable(1);
    public static final IntWritable SALARY_RECORD = new IntWritable(0);
}
