package com.phoenixopsgroup.example.kafka.clients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
public abstract class AbstractConsumer<K,V> {
    public void run() {

        final KafkaConsumer<K, V> consumer = createConsumer();
        consumer.subscribe(getConsumerTopics());

        setup();

        // Configure clean shutdown/ctrl-c handling
        final Thread t = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Stop kafka consumer/main thread
            consumer.wakeup();

            // Wait .5s for main to finish processing
            try {
                t.join(500);
            } catch (InterruptedException e){
                e.printStackTrace();
            }

            try {
                teardown();
            } catch (Exception e){
                // Ignore, log?
                e.printStackTrace();
            }
        }));


        try {
            while (true) {
               consumer.poll(Duration.ofMillis(200)).forEach(this::process);
            }
        } catch (WakeupException e){
            //Ignore, closing
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

    protected KafkaConsumer<K, V> createConsumer() {
        return new KafkaConsumer<>(getConsumerProperties());
    }

    protected abstract Properties getConsumerProperties();

    protected abstract Set<String> getConsumerTopics();

    protected void setup() {

    }

    protected void teardown() {

    }

    protected abstract void process(ConsumerRecord<K, V> record);
}
