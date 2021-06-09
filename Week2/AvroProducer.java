
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class AvroProducer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.6:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://10.140.0.3:8081");
        KafkaProducer producer = new KafkaProducer(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("C:\\Users\\DELL\\OneDrive\\Máy tính\\BTVN GHTK\\AvroSeDe\\src\\main\\java\\schema.avsc"));
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("username", "a");
        avroRecord.put("age", 2);
        avroRecord.put("phone", "01234567890");

        GenericRecord mailing = new GenericData.Record(schema.getField("address").schema());
        mailing.put("street" , "abc");
        mailing.put("city" , "abcd");
        mailing.put("country" , "abcde");

        avroRecord.put("address" , mailing);
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("topic1", null, avroRecord);

        producer.send(record);


//        try {
//            producer.send(record);
//        } catch(SerializationException e) {
//            // may need to do something with it
//            System.out.println(e);
//        }
//        finally {
            producer.flush();
            producer.close();
//        }
    }


}