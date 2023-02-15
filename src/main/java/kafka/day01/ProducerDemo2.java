package kafka.day01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo2 {
    public static void main(String[] args) throws InterruptedException {

        Properties pros = new Properties();
        //指定kafka集群的地址
        pros.setProperty("bootstrap.servers", "linux001:9092,linux002:9092,linux003:9092");
        //指定key的序列化方式
        pros.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定value的序列化方式
        pros.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //ack模式，取值有0，1，-1（all），all是最慢但最安全的  服务器应答生产者成功的策略
        pros.put("acks", "all");
        //这是kafka发送数据失败的重试次数，这个可能会造成发送数据的乱序问题
        pros.setProperty("retries", "3");
        //数据发送批次的大小 单位是字节
        pros.setProperty("batch.size", "10000");
        //一次数据发送请求所能发送的最大数据量
        pros.setProperty("max.request.size", "102400");
        //消息在缓冲区保留的时间，超过设置的值就会被提交到服务端
        pros.put("linger.ms", 10000);
        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        pros.put("buffer.memory", 10240);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(pros);
        for (int i = 0; i < 1000; i++) {
            //key value  0 --> doit32+-->+0
            //key value  1 --> doit32+-->+1
            //key value  2 --> doit32+-->+2
            //2.创建一些待发送的消息，构建成kafka所需要的格式
            ProducerRecord<String, String> record = new ProducerRecord<>("doit37", i + "", "doit32-->" + i);
            //3.调用kafka的api去发送消息
            kafkaProducer.send(record);
            Thread.sleep(100);
        }
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
