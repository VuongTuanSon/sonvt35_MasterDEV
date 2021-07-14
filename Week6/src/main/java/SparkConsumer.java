import com.google.protobuf.Message;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkConsumer{
    public static String getYear(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
    public static String getMonth(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("MM");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
    public static String getDay(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
    public static String getHour(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("HH");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }

    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("SparkKafka").getOrCreate();
        session.streams().awaitAnyTermination(1000);
        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","10.140.0.6:9092")
                .option("subscribe","data_tracking_sonvt35")
                .option("group.id","group1")
                .option("startingOffsets", "earliest")
                .option("auto.offset.reset","true")
                .option("value.serializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .load();
        Dataset<byte[]>words = df.select("value").as(Encoders.BINARY());
        Dataset<String> object = words.map((MapFunction<byte[], String>)
                s-> (Sonvt35.DataTracking.parseFrom(s).getVersion()+
                        "#"+ Sonvt35.DataTracking.parseFrom(s).getName()+
                        "#"+ getYear(Sonvt35.DataTracking.parseFrom(s).getTimestamp())+
                        "#"+ getMonth(Sonvt35.DataTracking.parseFrom(s).getTimestamp())+
                        "#"+ getDay(Sonvt35.DataTracking.parseFrom(s).getTimestamp())+
                        "#"+ getHour(Sonvt35.DataTracking.parseFrom(s).getTimestamp())+
                        "#"+ Sonvt35.DataTracking.parseFrom(s).getTimestamp()+
                        "#"+ Sonvt35.DataTracking.parseFrom(s).getPhoneId()+
                        "#"+ Sonvt35.DataTracking.parseFrom(s).getLon()+
                        "#"+ Sonvt35.DataTracking.parseFrom(s).getLat()
                ),Encoders.STRING());

        Dataset<Row> result = object
                .withColumn("version", functions.split(object.col("value"), "#").getItem(0))
                .withColumn("name", functions.split(object.col("value"), "#").getItem(1))
                .withColumn("year", functions.split(object.col("value"), "#").getItem(2))
                .withColumn("month", functions.split(object.col("value"), "#").getItem(3))
                .withColumn("day", functions.split(object.col("value"), "#").getItem(4))
                .withColumn("hour", functions.split(object.col("value"), "#").getItem(5))
                .withColumn("timestamp", functions.split(object.col("value"), "#").getItem(6))
                .withColumn("phone_id", functions.split(object.col("value"), "#").getItem(7))
                .withColumn("lon", functions.split(object.col("value"), "#").getItem(8))
                .withColumn("lat", functions.split(object.col("value"), "#").getItem(9))
                .drop("value");

        StreamingQuery query = result
                .writeStream()
                .format("parquet")
                .option("compression" , "snappy")
                .option("path","hdfs://10.140.0.5:9000/user/sonvt35/data_tracking")
                .option("checkpointLocation","hdfs://10.140.0.5:9000/user/sonvt35/checkpoint")
                .partitionBy("year" , "month" , "day" , "hour")
                .start();
        query.awaitTermination();
    }
}
