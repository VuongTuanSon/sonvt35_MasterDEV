import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

import java.io.*;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Random;

public class GenDataToFile {

    static Random rand = new Random();
    static Faker faker = new Faker();
    //generate random int data
    public static int getNumberBetween (int min, int max)
    {
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }
    //generate random phone number
    public static String getPhoneNumber()
    {
        String phoneNumber = faker.phoneNumber().cellPhone();
        return phoneNumber;
    }
    //generate full name
    public static String getFullName()
    {
        String fullName = faker.name().fullName();
        return fullName;
    }
    //generate random time


    public static Timestamp getPkgTime()
    {
        long rangebegin = Timestamp.valueOf("2019-01-01 00:00:00").getTime();
        long rangeend = Timestamp.valueOf("2021-06-14 23:59:59").getTime();
        long diff = rangeend - rangebegin +1;
        Timestamp rand = new Timestamp(rangebegin + (long)(Math.random() * diff));
        return rand;
    }

    public static void main(String[] args) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("src/main/java/data.csv"));
            writer.write("pkg_order,shop_code,customer_tel,customer_tel_normalize,fullname,pkg_created,pkg_modified,package_status_id,customer_province_id,customer_district_id,customer_ward_id,created,modified,is_cancel,ightk_user_id\n");
            for (int i = 0 ; i < 5000000 ; i++)
            {
                writer.write(i + ",");
                writer.write(getNumberBetween(1,100) + ",");
                writer.write(getPhoneNumber() + ",");
                writer.write(getPhoneNumber() + ",");
                writer.write(getFullName()+ ",");
                writer.write(String.valueOf(getPkgTime())+ ",");
                writer.write(String.valueOf(getPkgTime())+ ",");
                writer.write(getNumberBetween(1,100)+ ",");
                writer.write(getNumberBetween(1,100)+ ",");
                writer.write(getNumberBetween(1,100)+ ",");
                writer.write(getNumberBetween(1,100)+ ",");
                writer.write(getNumberBetween(1,100)+ ",");
                writer.write(getNumberBetween(1,100)+ "\n");
            }

            writer.close();

            System.out.println("done!");

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
