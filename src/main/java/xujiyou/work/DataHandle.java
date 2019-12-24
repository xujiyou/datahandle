package xujiyou.work;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * DataHandle class
 *
 * @author jiyouxu
 * @date 2019/12/19
 */
public class DataHandle {

    static ElasticsearchHandle elasticsearchHandle = new ElasticsearchHandle();
    static Neo4jHandle neo4jHandle = new Neo4jHandle();

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "fueltank-3:9092");
        props.put("group.id", "default_consumer_group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        new Thread(() -> {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("follower"));
            // 指定分区从末尾消费
            while (consumer.assignment().size() == 0) {
                consumer.poll(Duration.ofSeconds(1));
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
            while (consumer.assignment().size() == 0) {
                consumer.poll(Duration.ofSeconds(1));
            }
            consumer.seekToEnd(consumer.assignment());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String, String> record : records) {
                    String followerStr = record.value().replaceAll("None", "null");
                    followerStr = followerStr.replaceAll("False", "false");
                    followerStr = followerStr.replaceAll("True", "true");

                    System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + followerStr);
                    elasticsearchHandle.handleFollower(followerStr);
                    neo4jHandle.handleFollower(followerStr);
                }
            }
        }).start();

        new Thread(() -> {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("user_detail"));
            // 指定分区从末尾消费
            while (consumer.assignment().size() == 0) {
                consumer.poll(Duration.ofSeconds(1));
            }
            consumer.seekToEnd(consumer.assignment());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String, String> record : records) {
                    String userStr = record.value().replaceAll("None", "null");
                    userStr = userStr.replaceAll("False", "false");
                    userStr = userStr.replaceAll("True", "true");

                    System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + userStr);
                    elasticsearchHandle.handleUser(userStr);
                    neo4jHandle.handleUser(userStr);
                }
            }
        }).start();
    }
}
