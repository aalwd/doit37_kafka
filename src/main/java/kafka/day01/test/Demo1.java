package kafka.day01.test;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Demo1 {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        // 设置引入服务器, k,v的序列化类
        // 以及确认机制
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer1 = new KafkaProducer(props);



        for (int i = 0; i < 1000; i++) {
            if(i % 2 == 0) {
                producer1.send(new ProducerRecord<>("doit37", "test in even : " + i));
            } else {
                producer1.send(new ProducerRecord<>("t1", "test in odd : " + i ));
            }
            Thread.sleep(80);
        }

        producer1.flush();
        producer1.close();
    }
}
