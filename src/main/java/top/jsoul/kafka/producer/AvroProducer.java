package top.jsoul.kafka.producer;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) throws IOException {

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        GenericData.Record user1 = new GenericData.Record(schema);
        user1.put("name","Lily");
        user1.put("favorite_number",256);
        GenericData.Record user2 = new GenericData.Record(schema);
        user2.put("name","Gnosis");
        user2.put("favorite_number",1024);
        user2.put("favorite_color","black");

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.setProperty("group.id", "CustomerCounter");

        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(kafkaProps);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

        ProducerRecord<String, byte[]> record_send1 = new ProducerRecord<String,byte[]>("CustomerCountry", (String) user1.get("name"), user1);
        ProducerRecord<String, byte[]> record_send2 = new ProducerRecord<String,byte[]>("CustomerCountry", (String) user2.get("name"), user2);

        try {
            producer.send(record_send1);
            producer.send(record_send2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        consumer.subscribe(Collections.singletonList("CustomerCountry"));
        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
