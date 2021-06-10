import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


/**
 * Reads an avro message.
 */
public class AvroConsumer2 {

    public static void main(String[] str) throws InterruptedException, IOException {

        readMessages();


    }

    private static void readMessages() throws InterruptedException, IOException {

        KafkaConsumer<String, byte[]> consumer = createConsumer();

        // Assign to specific topic and partition, subscribe could be used here to subscribe to all topic.
        consumer.assign(Arrays.asList(new TopicPartition("topic1", 0)));

        processRecords(consumer);
    }

    private static void processRecords(KafkaConsumer<String, byte[]> consumer) throws InterruptedException, IOException {

        while (true) {

            ConsumerRecords<String, byte[]> records = consumer.poll(100);

            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = AvroSupport.byteArrayToData(AvroSupport.getSchema(), record.value());
                String firstName = AvroSupport.getValue(genericRecord, "username", String.class);
                System.out.println(firstName);

            }

            consumer.commitSync();
            Thread.sleep(500);

        }
    }

    private static KafkaConsumer<String, byte[]> createConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.140.0.6:9092");
        String consumeGroup = "cg1";
        props.put("group.id", consumeGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "100");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return new KafkaConsumer<String, byte[]>(props);
    }


}
