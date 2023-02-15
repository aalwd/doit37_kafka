package kafka.day01;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminApi {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers","linux001:9092,linux002:9092,linux003:9092");

        AdminClient adminClient = KafkaAdminClient.create(props);

        ListTopicsResult rs = adminClient.listTopics();

        for (String topic : rs.names().get()) {
            System.out.println(topic);
        }

//        NewTopic topic = new NewTopic("abc", 3, (short) 3);
//        adminClient.createTopics(Collections.singletonList(topic));
    }
}
