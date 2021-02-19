package top.jsoul.kafka.producer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        kafkaProps.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("group.id","CustomerCounter");
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(kafkaProps);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
        ProducerRecord<String, String> record_send = new ProducerRecord<String, String>("CustomerCountry" ,"Precision Products","France");
        try {
            producer.send(record_send);
        } catch (Exception e){
            e.printStackTrace();
        }

        consumer.subscribe(Collections.singletonList("CustomerCountry"));
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(),record.partition(),record.offset(),record.key(),record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

}
