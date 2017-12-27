import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();

        //boot strap server
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        //group id
        properties.setProperty("group.id","test");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("auto.offset.reset","earliest");

        Consumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("first_topic"));

        while (true){
            ConsumerRecords<String,String> crs = consumer.poll(100);

            for (ConsumerRecord<String,String> cr : crs){
                System.out.println("Key: " + cr.key() + ",value:" + cr.value() +
                                    ",partition:" + cr.partition() + ",offset:"+cr.offset() +
                                    ",topic:"+cr.topic() +",timestamp:"+cr.timestamp());
            }


        }


    }
}
