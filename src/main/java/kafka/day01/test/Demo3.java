package kafka.day01.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


public class Demo3 {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG , "a1");

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG , "5000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");



        // 设定要读取的信息
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        TopicPartition doit37 = new TopicPartition("doit37", 1);
        TopicPartition doit = new TopicPartition("doit37", 2);
        TopicPartition doit2 = new TopicPartition("doit37", 0);

        consumer.assign(Arrays.asList(doit37, doit2, doit));

        consumer.seek(doit2, 30);
        consumer.seek(doit37, 30);
        consumer.seek(doit, 30);


        while(true) {
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
