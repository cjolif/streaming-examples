import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class Example {
  public static void main(String[] args) {
    KStreamBuilder builder = new KStreamBuilder();

    Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    Serializer<JsonNode> jsonSerializer = new JsonSerializer();

    Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    Properties streamingConfig = new Properties();

    streamingConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaAvgJob");
    streamingConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamingConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    KStream<String, JsonNode> stream = builder.stream(Serdes.String(), jsonSerde, "origin");
    KGroupedStream<String, JsonNode> group = stream.groupBy((key, value) -> key,
            Serdes.String(), jsonSerde);
    KTable<String, JsonNode> countAndSum = group.aggregate(
            () -> {
                ObjectNode node = JsonNodeFactory.instance.objectNode();
                node.put("count", 0);
                node.put("sum", 0);
                return (JsonNode)node;
            },
            (key, value, aggregate) -> {
                // if we want to clear the state when we get the "last" event of this sort
                // we can "just" return null here
                /*
                if (eventLastOfItskind) return null;
                 */
                ((ObjectNode)aggregate).put("count",
                        aggregate.get("count").asLong() + 1);
                ((ObjectNode)aggregate).put("sum",
                        aggregate.get("sum").asLong() + value.get("value").asLong());
                return aggregate;
            },
            jsonSerde, "sumAndCount");
   KTable<String, JsonNode> average =
            countAndSum.mapValues((value -> {
              ObjectNode node = JsonNodeFactory.instance.objectNode();
              node.put("avg",
                      value.get("sum").asLong() / (double)value.get("count").asLong());
              return node;
            }));
    average.to(Serdes.String(), jsonSerde, "kafka-destination");

    KafkaStreams streams = new KafkaStreams(builder, streamingConfig);
    streams.start();
  }
}
