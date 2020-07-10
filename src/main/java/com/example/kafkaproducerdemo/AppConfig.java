package com.example.kafkaproducerdemo;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

@Configuration
public class AppConfig {

    public static final String SECURE_TOPIC_CONNECTION = "secureTopicConnection";
    public  static int counter = 0;



    @Bean
    KafkaProducer<String, String> buildKafkaProducer(Environment environment) {
        Properties producerConfig = new Properties();
        //producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        setupKafkaSSLproperties(environment, producerConfig);
        System.out.println(producerConfig);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer());
        return kafkaProducer;
    }

    private void setupKafkaSSLproperties(Environment environment, Properties properties) {
        if ("true".equals(environment.getProperty(SECURE_TOPIC_CONNECTION, "false"))) {

            String securityProtocol = environment.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

            String sslTruststoreLocation = environment.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            Objects.requireNonNull(sslTruststoreLocation, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);

            String sslTruststorePassword = environment.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
            Objects.requireNonNull(sslTruststorePassword, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

            String sslKeystoreLocation = environment.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
            Objects.requireNonNull(sslKeystoreLocation, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);

            String sslKeystorePassword = environment.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
            Objects.requireNonNull(sslKeystorePassword, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);

            String sslKeyPassword = environment.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
            Objects.requireNonNull(sslKeyPassword, SslConfigs.SSL_KEY_PASSWORD_CONFIG);

            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
            properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
            properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
            properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        }
    }


    @Order(1)
    @Bean
    ApplicationRunner buildKafkaProducerRunner(KafkaProducer<String, String> kafkaProducer, Environment environment) {
       return new ApplicationRunner() {
           @Override
           public void run(ApplicationArguments args) throws Exception {
               System.err.println("Producer Application runner ...");

               //below is my specific need ... publish all files under "producer.files" where filename (without extension) to be message key
               String producerFilePath = environment.getProperty("producer.files");
               System.out.println("producerFilePath="+producerFilePath);
               File file = new File(producerFilePath);
               File readFile = null;
               String message = null;
               for(String fileName  : file.list()) {

                   readFile = new File(file.getAbsolutePath() + File.separator + fileName);

                   String kafkaMessageKey = fileName.substring(0,fileName.indexOf('.'));
                   System.out.println("file full path=" + readFile);
                   System.out.println("file name=" + kafkaMessageKey);
                   if(readFile.isFile()) {
                       message = new String(FileCopyUtils.copyToByteArray(readFile));
                       //String topicName = environment.getProperty("topic-name", "my-demo-topic");
                       String topicName = environment.getProperty("topic-name");
                       ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, kafkaMessageKey , message);
                       RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                       counter++;
                       System.out.println("processed file # "+ counter + " filename="+fileName);
//                       System.out.println(
//                                                    "topic =" + recordMetadata.topic() + " " +
//                                                    "partition =" + recordMetadata.partition() + " " +
//                                                    "offset =" + recordMetadata.offset() + " " +
//                                                    "timestamp=" + recordMetadata.timestamp() + " " +
//                                                    "serializedKeySize="+ recordMetadata.serializedKeySize() + " " +
//                                                    "serializedValueSize="+ recordMetadata.serializedValueSize() + " "
//                                            );

                       System.out.println(
                               "topic =" + recordMetadata.topic() + " " +
                                       "partition =" + recordMetadata.partition() + " " +
                                       "offset =" + recordMetadata.offset() + " "




                       );

                   }
               }

           }
       };

    }
}
