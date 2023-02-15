package kafka.day01.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Demo2 {
    public static void main(String[] args) {

        Properties props = new Properties();

        // 设置引导服务器, k,v 的反序列化类的权限定类名 , 以及必选像: 组名
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "a1");

        // 可选项

        // 设置是否自动提交offset, 以及每次提交的间隔时间
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");


        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        // 消费者订阅topic
        consumer.subscribe(Arrays.asList("doit37", "t1"));

        while(true) {
            // 拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                long offset = record.offset();
                int partition = record.partition();
                String topic = record.topic();
                String value = record.value();
                String key = record.key();

                long timestamp = record.timestamp();

                System.out.println("offset : " + offset + " partititon : " + partition
                        + " topic : " + topic + " key : " + key + " value : " + value + " timestamp : " + timestamp);
            }


        }





    }
}
