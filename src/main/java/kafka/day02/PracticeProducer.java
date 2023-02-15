package kafka.day02;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class PracticeProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        Event event = new Event();

        while(true) {
            event.setGuid(RandomUtils.nextInt(0,10000));
            event.setEvent_id(RandomStringUtils.randomAlphabetic(10));
            event.setTimestamp(System.currentTimeMillis());


            String json = JSON.toJSONString(event);

            producer.send(new ProducerRecord<String, String>("event-log", json));
            Thread.sleep(new Random().nextInt(110));
        }
    }
}
