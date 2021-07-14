import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

public class ProtobufProducer {
    public static void main(String[] args) throws IOException {
        Faker faker = new Faker();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.140.0.6:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);
        ProducerRecord<String, byte[]> producerRecord = null;

        for(int i = 20 ; i < 5000 ; i++){
            Sonvt35.DataTracking message = Sonvt35.DataTracking.newBuilder()
                    .setVersion(String.valueOf(i))
                    .setName(faker.name().fullName())
                    .setTimestamp(faker.number().numberBetween(1563074884, 1752463684))
                    .setPhoneId(faker.phoneNumber().phoneNumber())
                    .setLon(faker.number().numberBetween(-180,180))
                    .setLat(faker.number().numberBetween(-90,90))
                    .build();
            producerRecord = new ProducerRecord<String, byte[]>("data_tracking_sonvt35", null, message.toByteArray());
            producer.send(producerRecord);
        }
//


        producer.flush();
        producer.close();


    }
}