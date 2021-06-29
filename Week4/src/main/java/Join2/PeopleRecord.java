package Join2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PeopleRecord implements Writable {
    public Text ID = new Text();
    public Text firstName = new Text();
    public Text lastName = new Text();
    public Text email = new Text();
    public Text city = new Text();
    public Text profession = new Text();
    public Text fieldName = new Text();

    public PeopleRecord(){}
    public PeopleRecord(String ID, String firstName, String lastName, String email, String city, String profession, String fieldName) {
        this.ID.set(ID);
        this.firstName.set(firstName);
        this.lastName.set(lastName);
        this.email.set(email);
        this.city.set(city);
        this.profession.set(profession);
        this.fieldName.set(fieldName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ID.write(out);
        firstName.write(out);
        lastName.write(out);
        email.write(out);
        city.write(out);
        profession.write(out);
        fieldName.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        ID.readFields(in);
        firstName.readFields(in);
        lastName.readFields(in);
        email.readFields(in);
        city.readFields(in);
        profession.readFields(in);
        fieldName.readFields(in);
    }
}
