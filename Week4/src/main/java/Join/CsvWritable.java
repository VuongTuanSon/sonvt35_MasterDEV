package Join;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CsvWritable implements Writable {
    private Text ID = new Text();
    private Text firstName = new Text();
    private Text lastName = new Text();
    private Text email = new Text();
    private Text city = new Text();
    private Text profession = new Text();
    private Text fieldName = new Text();
    private Text salary = new Text();

    @Override
    public void write(DataOutput out) throws IOException {
        ID.write(out);
        firstName.write(out);
        lastName.write(out);
        email.write(out);
        city.write(out);
        profession.write(out);
        fieldName.write(out);
        salary.write(out);
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
        salary.readFields(in);
    }

    public Text getID() {
        return ID;
    }
    public void setID(Text ID) {
        this.ID = ID;
    }
    public Text getFirstName() {
        return firstName;
    }
    public void setFirstName(Text firstName) {
        this.firstName = firstName;
    }
    public Text getLastName() {
        return lastName;
    }
    public void setLastName(Text lastName) {
        this.lastName = lastName;
    }
    public Text getEmail() {
        return email;
    }
    public void setEmail(Text email) {
        this.email = email;
    }
    public Text getCity() {
        return city;
    }
    public void setCity(Text city) {
        this.city = city;
    }
    public Text getFieldName() {
        return fieldName;
    }
    public void setFieldName(Text fieldName) {
        this.fieldName = fieldName;
    }
    public Text getProfession() {
        return profession;
    }
    public void setProfession(Text profession) {
        this.profession = profession;
    }
    public Text getSalary() {
        return salary;
    }
    public void setSalary(Text salary) {
        this.salary = salary;
    }
}
