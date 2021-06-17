import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;


public class GenerateInsert {
    static Faker faker = new Faker(new Locale("vi"));
    static Random rand = new Random();
    public static int getShopCode()
    {
        List <Integer> list = new ArrayList<>();
        for (int i = 1 ;i < 100 ;i++)
        {
            list.add(i);
        }
        return list.get(rand.nextInt(list.size()));
    }
    public static String getFullName()
    {
        String fullName = faker.name().fullName();
        return fullName;
    }
    public static String getPhoneNumber()
    {
        String phoneNumber = faker.phoneNumber().cellPhone();
        return phoneNumber;
    }
    public static Timestamp getPkgTime()
    {
        long rangebegin = Timestamp.valueOf("2019-01-01 00:00:00").getTime();
        long rangeend = Timestamp.valueOf("2021-06-14 23:59:59").getTime();
        long diff = rangeend - rangebegin +1;
        Timestamp rand = new Timestamp(rangebegin + (long)(Math.random() * diff));
        return rand;
    }
    public static int getStatus()
    {
        List<Integer> list = new ArrayList<>();
        list.add(0);
        list.add(1);
        list.add(2);
        list.add(3);
        return list.get(rand.nextInt(list.size()));
    }
    public static int getProvinceID()
    {
        List <Integer> list = new ArrayList<>();
        for (int i = 1 ;i < 65 ;i++)
        {
            list.add(i);
        }
        return list.get(rand.nextInt(list.size()));
    }
    public static int getDistrictID()
    {
        List <Integer> list = new ArrayList<>();
        for (int i = 1 ;i < 12 ;i++)
        {
            list.add(i);
        }
        return list.get(rand.nextInt(list.size()));
    }
    public static int getWardID()
    {
        List <Integer> list = new ArrayList<>();
        for (int i = 1 ;i < 300 ;i++)
        {
            list.add(i);
        }
        return list.get(rand.nextInt(list.size()));
    }
    public static int getUserGHTKID()
    {
        List <Integer> list = new ArrayList<>();
        for (int i = 1 ;i < 1000 ;i++)
        {
            list.add(i);
        }
        return list.get(rand.nextInt(list.size()));
    }
    public static int getIsCancel()
    {
        List <Integer> list = new ArrayList<>();
        for (int i = 0 ;i < 2 ;i++)
        {
            list.add(i);
        }
        return list.get(rand.nextInt(list.size()));
    }
    public static void main(String[] args)
    {
        try
        {
            long startTime = System.nanoTime();
            // create a mysql database connection
            String myUrl = "jdbc:mysql://localhost/ghtkdb";
            Connection conn = DriverManager.getConnection(myUrl, "root", "");


            // the mysql insert statement
            String query = " insert into customers_packages (pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created, pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, is_cancel, ightk_user_id)"
                    + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            // create the mysql insert preparedstatement
            PreparedStatement preparedStmt = conn.prepareStatement(query);
            for (int i = 5000000 ; i<=5000000 ; i++) {
                preparedStmt.setInt(1, i);
                preparedStmt.setInt(2, getShopCode());
                preparedStmt.setString(3, getPhoneNumber());
                preparedStmt.setString(4, getPhoneNumber());
                preparedStmt.setString(5, getFullName());
                preparedStmt.setTimestamp(6, getPkgTime());
                preparedStmt.setTimestamp(7, getPkgTime());
                preparedStmt.setInt(8, getStatus());
                preparedStmt.setInt(9, getProvinceID());
                preparedStmt.setInt(10, getDistrictID());
                preparedStmt.setInt(11, getWardID());
                preparedStmt.setInt(12, getIsCancel());
                preparedStmt.setInt(13, getUserGHTKID());
                preparedStmt.execute();
            }

            // execute the preparedstatement
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
