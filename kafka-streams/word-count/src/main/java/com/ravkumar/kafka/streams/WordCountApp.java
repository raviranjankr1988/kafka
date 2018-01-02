package com.ravkumar.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-app");


        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        /*   1 - stream from Kafka
             2 - map values to lowercase
             3 - flatmap values split by space
             4 - select key to apply a key (we discard the old key)
             5 - group by key before aggregation
             6 - count occurences
             7 - to in order to write the results back to kafka
        */

        StreamsBuilder builder = new StreamsBuilder();

        //1 - stream from Kafka
        KStream<String,String> wordCountInput = builder.stream("word-count-input");

        //2 - map values to lowercase
        KTable<String,Long> wordCounts = wordCountInput
                .mapValues(value -> value.toLowerCase())
                //3 - flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                 //4 - select key to apply a key (we discard the old key)
                .selectKey((key,value) -> value)
                //5 - group by key before aggregation
                .groupByKey()
                //6 - count occurences
                .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

        //7 - to in order to write the results back to kafka
        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.start();

        //print the topology
        System.out.println(streams);

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
