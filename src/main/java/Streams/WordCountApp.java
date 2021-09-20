package Streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties propertiesConfig = new Properties();
        propertiesConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count");
        propertiesConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        propertiesConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        propertiesConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        propertiesConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> wordCountInput = streamsBuilder.stream("word-count-input");
        KTable<String,Long> wordCountOutput = wordCountInput.mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(lowerCaseLine -> Arrays.asList(lowerCaseLine.split(" ")))
                .selectKey((ignoredKey,word) -> word)
                .groupByKey()
                .count(Materialized.as("Counts"));
        wordCountOutput.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),propertiesConfig);
        kafkaStreams.start();
        System.out.println(kafkaStreams);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        while(true){
            kafkaStreams.localThreadsMetadata().forEach(System.out::println);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
