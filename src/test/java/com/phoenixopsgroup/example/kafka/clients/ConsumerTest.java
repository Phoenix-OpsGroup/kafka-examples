package com.phoenixopsgroup.example.kafka.clients;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.io.IOException;
import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.*;
import org.apache.commons.io.IOUtils;


public class ConsumerTest {

    @Before
    public void setup() throws IOException {
       loaddata();
    }


   @Test
    public void ConsumerTestMain() throws IOException {
      String[] args=null;
      //SimpleConsumer.main(args); //Basic Consumer
      Consumer.main(args);

   }

 @Test
    public void loaddata() throws IOException {
       String config = "conf/producer.properties";
       String producerTopic = "test";
       //get the properties of the producer from the properties file found on the classpath
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource(config).openStream()) {
           Properties properties = new Properties();
           properties.load(props);
           //Create the Producer
           producer = new KafkaProducer<>(properties);
       }

       //Load Test KML Data
       String relFilePath = "mfws.kml";
       ClassLoader loader = Thread.currentThread()
               .getContextClassLoader();
       URL srcUrl = loader.getResource(relFilePath);
       if (srcUrl == null) {
           throw new RuntimeException("Bad test file path [" + relFilePath + "]");
       }

       InputStream is = null;
       try {
           is = new FileInputStream(srcUrl.getFile());
           String xml = IOUtils.toString(is, "UTF-8");  //Inputstream to String
           //System.out.println(xml);
           producer.send(new ProducerRecord<String, String>( producerTopic ,"1",xml));
       } catch (FileNotFoundException ex) {
           throw new RuntimeException(ex);
       } catch (IOException e) {
           // TODO Auto-generated catch block
           e.printStackTrace();
       }
       finally {
           producer.close();
       }
   }



}
