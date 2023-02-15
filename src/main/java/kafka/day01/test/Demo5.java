package kafka.day01.test;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Demo5 {
    public static void main(String[] args) throws SQLException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql", "root", "123456");
        conn.setAutoCommit(false);

        PreparedStatement detail = conn.prepareStatement("insert into kafka values(?,?,?)");
        PreparedStatement off = conn.prepareStatement("insert into kafka_offset values(? , ?) on duplicate key update offset=?");
        PreparedStatement getOffset = conn.prepareStatement("select offset from kafka_offset where topic_partition = ?");

        Properties props = new Properties();
        // gid boots , k, v des
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "d1");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        consumer.subscribe(Collections.singletonList("t2"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    int p = partition.partition();
                    String t = partition.topic();

                    // 设置偏移量
                    long offset = -1L;

                    try {
                        getOffset.setString(1, t + "_" + p);
                        ResultSet rs = getOffset.executeQuery();

                        while(rs.next()) {
                            offset = rs.getLong(1);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        // 当查不到数据时就从0开始
                        consumer.seek(partition, offset+1);
                    }


                }
            }
        });



        // catch 中的rollback的主要作用：
        // mysql在存储数据的时候， 在有异常的时候，  使用rollback， ， 会直接跳过 ， 不存储offset和数据
        // 但是如果后面的数据在执行的时候， 没有抛出异常， 则会继续下移offset ， 将offset和数据继续存储在mysql中，
        /**
         * 这样可以不仅可以实现脏数据过滤， 也可以避免重复读的问题
         */
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    String[] arr = record.value().split(",");
                    int id = Integer.parseInt(arr[0]);
                    String name = arr[1];
                    int age = Integer.parseInt(arr[2]);
                    detail.setInt(1, id);
                    detail.setString(2, name);
                    detail.setInt(3, age);

                    detail.executeUpdate();

                    String topicAndPartition = record.topic() + "_" + record.partition();
                    long offset = record.offset();

                    off.setString(1, topicAndPartition);
                    off.setLong(2, offset);
                    off.setLong(3, offset);
                    off.executeUpdate();

                    conn.commit();

                } catch (Exception e) {
                    conn.rollback();

                    System.out.println("something wrong!!!");
                    e.printStackTrace();
                }
            }
        }



    }
}
