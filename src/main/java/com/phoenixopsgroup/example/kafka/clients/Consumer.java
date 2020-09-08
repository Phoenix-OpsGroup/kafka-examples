package com.phoenixopsgroup.example.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import com.google.common.io.Resources;

public class Consumer extends AbstractConsumer<String,String> {
    private static String TOPIC = "test";
    @Override
    protected Properties getConsumerProperties() {
        try (InputStream props = Resources.getResource("conf/consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            return properties;
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }




    @Override
    protected Set<String> getConsumerTopics() {
        return Collections.singleton(TOPIC);
    }

    public static void main(String... args){
        // TODO Arg validation

        Consumer consumer = new Consumer();
        consumer.run();
    }

    @Override
    protected void process(ConsumerRecord<String, String> record) {
        System.out.println("Record received partition : " + record.partition() + ", key : " + record.key() + ", value :  " + record.value() + ", offset : " + record.offset());
    }
}
