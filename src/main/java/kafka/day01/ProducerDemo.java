package kafka.day01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();


        // 以下是必须配的
//        props.put("bootstrap.servers", "linux001:9092");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");
        props.put("key.serializer", StringSerializer.class.getName());

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 可选的:

        // acks kafka的生产者的应答方式 [all, 0 , -1, 1]
//        props.put("acks", "-1");

        props.put(ProducerConfig.ACKS_CONFIG, "-1");





        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

//        ProducerRecord<String,String> record = new ProducerRecord<>("doit37", "12333");
//        producer.send(record);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("doit37", "hello" + i));
            Thread.sleep(50);
        }

        producer.flush();
        producer.close();

    }
}
