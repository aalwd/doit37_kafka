package kafka.day02;

import com.alibaba.fastjson.JSON;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BloomFilterConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        // 创建布隆过滤器，
        // 使用布隆过滤器， 来判断当前用户是否存在
        BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 10000);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g01");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("event-log"));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));

            for (ConsumerRecord<String, String> record : records) {

                String value = record.value();

                MyEvent myEvent = JSON.parseObject(value, MyEvent.class);

                boolean flag = bf.mightContain((long)myEvent.getGuid());

                if(flag) {
                    // 如果存在， 代表已经存在， 是老用户， 就将isNew设置为0
                    myEvent.setIsNew(0);
                } else {
                    // 如果不存在， 就在布隆过滤器中进行添加
                    bf.put((long)myEvent.getGuid());
                }


            }
        }

    }
}



@Getter
@Setter
@AllArgsConstructor
@ToString
@NoArgsConstructor
class MyEvent {
    private int guid;
    private String event_id;
    private long timestamp;

    private int isNew = 1;
}