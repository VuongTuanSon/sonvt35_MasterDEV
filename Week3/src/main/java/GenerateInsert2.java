import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;


public class GenerateInsert2 {
    int count = 0;
    static Faker faker = new Faker(new Locale("vi"));
    static Random rand = new Random();
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
    public static void main(String[] args)
    {
        try
        {
            int count = 0;
            long startTime = System.nanoTime();

            String myUrl = "jdbc:mysql://localhost/ghtk";
            Connection conn = DriverManager.getConnection(myUrl, "root", "");

            String query = " insert into customers_packages (pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created, pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, is_cancel, ightk_user_id)"
                    + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            int BATCH_SIZE = 100000;

            PreparedStatement preparedStmt = conn.prepareStatement(query);
            for (int i = 1 ; i<=5000000 ; i++) {
                preparedStmt.setInt(1, i);
                preparedStmt.setInt(2, getNumberBetween(1,100));
                preparedStmt.setString(3, getPhoneNumber());
                preparedStmt.setString(4, getPhoneNumber());
                preparedStmt.setString(5, getFullName());
                preparedStmt.setTimestamp(6, getPkgTime());
                preparedStmt.setTimestamp(7, getPkgTime());
                preparedStmt.setInt(8, getNumberBetween(0,3));
                preparedStmt.setInt(9, getNumberBetween(1,64));
                preparedStmt.setInt(10, getNumberBetween(0,100));
                preparedStmt.setInt(11, getNumberBetween(0,100));
                preparedStmt.setInt(12, getNumberBetween(0,1));
                preparedStmt.setInt(13, getNumberBetween(0,1000));
                preparedStmt.addBatch();
                if (i % BATCH_SIZE == 0)
                {
                    preparedStmt.executeBatch();
                    System.out.println("batch" + count++);
                }
            }
            preparedStmt.executeBatch();
            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            double durationSec = (double) duration / 1_000_000_000;
            System.out.println(durationSec);
            conn.close();
        }
        catch (Exception e)
        {
            System.err.println("Got an exception!");
            System.err.println(e.getMessage());
        }
    }
}
