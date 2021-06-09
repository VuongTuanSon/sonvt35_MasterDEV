import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class ProducerFIle {
    public static void main(String [] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.140.0.6:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;
        String line = "";
        KafkaProducer kafkaProducer = null;
        try {
            fis = new FileInputStream("C:\\Users\\DELL\\OneDrive\\Máy tính\\BTVN GHTK\\ProducerConsumer\\src\\main\\java\\Data.txt");
            //Construct BufferedReader from InputStreamReader
            br = new BufferedReader(new InputStreamReader(fis));
            kafkaProducer = new KafkaProducer(properties);
            while ((line = br.readLine()) != null) {
                lineCount++;
                kafkaProducer.send(new ProducerRecord("sonvt35_test" , Integer.toString(lineCount), line));
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{

                kafkaProducer.close();

        }

    }
}
