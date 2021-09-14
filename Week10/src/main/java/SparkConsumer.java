
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class SparkConsumer{
    public static void main(String[] args) throws Exception {
        //Các parameter để kết nối với kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.140.0.5:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "sonvt35");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        //Tên topic
        Collection<String> topics = Arrays.asList("sonvt351.sonvt35.user");

        SparkConf sparkConf = new SparkConf();

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        //Consume dữ liệu từ topic kafka
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
        //Lấy ra dữ liệu trong cột oid để làm key, sau đó
        JavaPairDStream<String, String> DStreamPair = stream.mapToPair(record -> {
                    JSONObject jsonObject = new JSONObject(record.value());
                    JSONObject payloadObject = jsonObject.getJSONObject("payload");
                    String oid = payloadObject.getString("$oid");
            return new Tuple2<>(oid, record.value());
        });
        //Chuyển JavaPairDStream có sẵn thành các PairRDD rồi sau đó biến chúng thành Dataset để tương tác dễ hơn
        DStreamPair.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>(){
            @Override
            public void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
                JavaRDD<Row> rowRdd = stringStringJavaPairRDD.map(new Function<Tuple2<String, String>, Row>() {
                    @Override
                    public Row call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        Row row = RowFactory.create(stringStringTuple2);
                        return row;
                    }
                });

                //Tạo schema cho Dataset
                StructType schema = DataTypes.createStructType(new StructField[] {DataTypes.createStructField("key", DataTypes.StringType, true),
                                                                                    DataTypes.createStructField("Message", DataTypes.StringType, true)});

                SparkSession spark = JavaSparkSessionSingleton.getInstance(stringStringJavaPairRDD.context().getConf());
                //Tạo Dataset với PairRDD và schema
                Dataset<Row> msgDataFrame = spark.createDataFrame(rowRdd, schema);
                //Thêm cột day để partition theo ngày
                msgDataFrame = msgDataFrame.withColumn("day" , functions.lit(java.time.LocalDate.now()));
                //Điều kiện kiểm tra path đã tồn tại chưa
                try {
                    FileSystem fileSystem = (new Path("hdfs://10.140.0.5:9000/user/sonvt35/kafka-connect")).getFileSystem(new Configuration());
                    //Nếu tồn tại rồi thì lấy Dataset trong đó và thực hiện merge với Dataset mới
                    if (fileSystem.exists(new Path("hdfs://10.140.0.5:9000/user/sonvt35/kafka-connect/day=2021-08-21")))
                    {
                        Dataset<Row> msgDataFrameOld = spark.read().parquet("hdfs://10.140.0.5:9000/user/sonvt35/kafka-connect/day=2021-08-21/*.parquet");
                        msgDataFrameOld.createOrReplaceTempView("oldDataFrame");
                        msgDataFrame.createOrReplaceTempView("newDataFrame");
                        Dataset<Row> finalDataFrame = spark.sql("select * from oldDataFrame left join newDataFrame on oldDataFrame.key = newDataFrame.key   ");
                        finalDataFrame.write().partitionBy("day").mode(SaveMode.Append).parquet("hdfs://10.140.0.5:9000/user/sonvt35/kafka-connect");
                    }
                } catch (IOException e) {
                    //Nếu chưa thì viết luôn Dataset mới vào HDFS
                    msgDataFrame.write().partitionBy("day").mode(SaveMode.Append).parquet("hdfs://10.140.0.5:9000/user/sonvt35/kafka-connect");
                }

            }
        });
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}

