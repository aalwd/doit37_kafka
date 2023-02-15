package kafka.day01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ComsumerDemo2 {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // api里 groupid必须写

        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g01");

        // 是否自动创建一个topic
        props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");

        // 是否充值消费者偏移量, earliest 从最早的开始度, latest 从最新的开始读
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 自动提交偏移量
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");


        // 间隔多长时间提交一次偏移量
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");


        TopicPartition partition1 = new TopicPartition("doit37", 0);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 找到指定topic 中的分区的offset 来开始读取数据
        consumer.assign(Arrays.asList(partition1));

        consumer.seek(partition1, 24);

        // 使用consumer的poll方法来拉取数据
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

        consumer.close();


    }

}
