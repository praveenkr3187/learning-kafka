package kafkaStreams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamFilter {
    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        
        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, v) -> 
                    extractUserFollwersInTweets(v) > 100
        );
        
        filteredStream.to("important_tweets");
        
        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start application
        kafkaStreams.start();
    }
    
    private static JsonParser jsonParser = new JsonParser();

    private static int extractUserFollwersInTweets(String v) {
        try{
            return jsonParser.parse(v).
                    getAsJsonObject().
                    get("user").
                    getAsJsonObject().
                    get("followers_count").
                    getAsInt();
        }catch (Exception e){
            e.printStackTrace();
            return -1;
        }
    }
}
