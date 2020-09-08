package com.phoenixopsgroup.example.kafka.clients;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import org.slf4j.Logger;

public class SimpleConsumer {
    static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);
    static KafkaConsumer<String, String> consumer=null;
    static String DEFAULT_CONSUMER_CONFIG = "conf/consumer.properties";
    static String DEFAULT_PRODUCER_CONFIG = "conf/producer.properties";
    public static void main(String[] args) throws IOException {

        //Counter
        int timeouts=0;

        configureConsumer(args);



        try
        {
            while(true)
            {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(timeouts));
                if(records.count()==0)
                {
                    timeouts++;
                }
                else
                {
                    LOGGER.info("Got " + records.count() + " records after " + timeouts + " timeouts\n");
                    timeouts = 0;  //reset counter
                    Iterator<ConsumerRecord<String,String>> iter = records.iterator();
                    while(iter.hasNext())
                    {
                        ConsumerRecord<String,String> record = iter.next();
                        String kml = record.value();
                        LOGGER.info(kml);
                        }
                    }
                }

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }


    }

    public static void configureConsumer(String[] args) throws IOException
    {
        String config = "conf/consumer.properties";
        String topic="test";
        try (InputStream props = Resources.getResource(config).openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            //If group.id is not found in properties file randomly create one
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "test");
            }

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }


    }

}
