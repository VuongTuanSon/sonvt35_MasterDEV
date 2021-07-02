package week5;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class sonvt35 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().parquet("/Sample_data");

        df.createOrReplaceTempView("contain_null");

        df = spark.sql("select * from contain_null where device_model is not null and user_id is not null");
        df.createOrReplaceTempView("non_device_model_null");

        Dataset<Row> device_model_num_user = spark.sql("select device_model, count(distinct(user_id)) as count from non_device_model_null group by device_model");
        device_model_num_user.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.5:9000/user/sonvt35/device_model_num_user");

        Dataset<Row> device_model_list_user = spark.sql("select device_model, concat_ws(',' , collect_list(user_id)) as list_user_id from non_device_model_null group by device_model");
        device_model_list_user.repartition(1).write().mode(SaveMode.Overwrite).option("compression" , "snappy").orc("hdfs://10.140.0.5:9000/user/sonvt35/device_model_list_user");

        df = spark.sql("select * from non_device_model_null where button_id is not null");
        df.createOrReplaceTempView("non_button_id_null");

        Dataset<Row> button_count_by_user_id_device_model = spark.sql("select concat(user_id, ',' , device_model) as user_id_device_model ,button_id, count(*) as count from non_button_id_null group by user_id, device_model, button_id");
        button_count_by_user_id_device_model.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.5:9000/user/sonvt35/button_count_by_user_id_device_model");
    }
}
