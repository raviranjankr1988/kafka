package com.ravkumar.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-color-app2");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> favouriteColorInput = builder.stream("favourite-color-input2");

        KStream<String,String> favouriteColor = favouriteColorInput
                .filter((key, value) -> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user,color) -> Arrays.asList("green", "blue", "red").contains(color));

        favouriteColor.to("favourite-color-intermediate2",Produced.with(Serdes.String(),Serdes.String()));

        KTable<String,String> favouriteColorInter = builder.table("favourite-color-intermediate2");

        KTable<String, Long> favouriteColours = favouriteColorInter
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count();

        favouriteColours.toStream().to("favourite-color-output2", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(),config);

        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        //print the topology
        System.out.println(streams);

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
