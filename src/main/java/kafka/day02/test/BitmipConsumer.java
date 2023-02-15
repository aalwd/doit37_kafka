package kafka.day02.test;

import com.alibaba.fastjson.JSON;
import kafka.day02.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.util.*;

public class BitmipConsumer {
    public static void main(String[] args) {
        RoaringBitmap map = RoaringBitmap.bitmapOf(10000);


        BitmipReader reader = new BitmipReader(map);
        reader.start();

        Timer timer = new Timer();

        timer.schedule(new Display(map),2000, 5000);


    }

}

class BitmipReader extends Thread {

    RoaringBitmap map;
    KafkaConsumer<String, String> consumer = null;

    public BitmipReader(RoaringBitmap map) {
        this.map = map;
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

        consumer = new KafkaConsumer<>(props);
    }


    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList("event-log"));



        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                Event event = JSON.parseObject(record.value(), Event.class);
                map.add(event.getGuid());
            }
        }

    }
}

// 显示数据
class Display extends TimerTask {
    RoaringBitmap map;

    public Display(RoaringBitmap map) {
        this.map = map;
    }

    @Override
    public void run() {
        System.out.println("所有数据的条数 ： " + map.getCardinality());
    }
}


