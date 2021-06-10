import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class ConsumerFile {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.140.0.6:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");
        properties.put("auto.offset.reset", "latest");
        int count_windmill , count_don, count_quixote, count_manuel, count_combat, count_man, count_woman, count_lady, count_mountain, count_sword;
        count_windmill = count_don = count_quixote = count_manuel = count_combat = count_man = count_woman = count_lady = count_mountain = count_sword = 0;
        String record_value;
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("sonvt35_test");
        kafkaConsumer.subscribe(topics);
        try{
            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord record: records) {
                    record_value = String.format("%s", record.value()).toLowerCase();
                    String[] split_value = record_value.split("\\W");
                    for (String w : split_value) {
                        if (w.equals("windmill"))
                        {
                            count_windmill++;
                        }
                        else if (w.equals("don"))
                        {
                            count_don ++;
                        }
                        else if (w.equals("quixote"))
                        {
                            count_quixote ++;
                        }
                        else if (w.equals("manuel"))
                        {
                            count_manuel ++;
                        }
                        else if (w.equals("combat"))
                        {
                            count_combat ++;
                        }
                        else if (w.equals("man"))
                        {
                            count_man ++;
                        }
                        else if (w.equals("woman"))
                        {
                            count_woman ++;
                        }
                        else if (w.equals("lady"))
                        {
                            count_lady ++;
                        }
                        else if (w.equals("mountain"))
                        {
                            count_mountain ++;
                        }
                        else if (w.equals("sword"))
                        {
                            count_sword ++;
                        }
                    }
                }
                System.out.println("windmill " + count_windmill);
                System.out.println("don " + count_don);
                System.out.println("quixote " + count_quixote);
                System.out.println("manuel " + count_manuel);
                System.out.println("combat " + count_combat);
                System.out.println("man " + count_man);
                System.out.println("woman " + count_woman);
                System.out.println("lady " + count_lady);
                System.out.println("mountain " + count_mountain);
                System.out.println("sword " + count_sword);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }

    }
}
