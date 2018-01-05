import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Properties;


public class Example {

  public static class AverageAccumulator {
    String key;
    long count;
    long sum;
  }

  // how can I clean the aggregate state if I know I won't receive any more events for it
  public static class Average implements AggregateFunction<ObjectNode, AverageAccumulator, Tuple2> {
    @Override
    public AverageAccumulator createAccumulator() {
      return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator add(ObjectNode value, AverageAccumulator accumulator) {
      // a little bit weird but we need to keep the key as part of the accumulator to be able
      // to serialize it back in the end
      accumulator.key = value.get("key").asText();
      accumulator.sum += value.get("value").get("value").asLong();
      accumulator.count++;
      return accumulator;
    }

    @Override
    public Tuple2<String, Double> getResult(AverageAccumulator accumulator) {
      return new Tuple2<>(accumulator.key, accumulator.sum / (double)accumulator.count);
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
      a.count += b.count;
      a.sum += b.sum;
      return a;
    }
  }

  public static class AverageSerializer implements KeyedSerializationSchema<Tuple2> {
    @Override
    public byte[] serializeKey(Tuple2 element) {
      return ("\"" + element.getField(0).toString() + "\"").getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2 element) {
      String value = "{\"avg\": "+ element.getField(1).toString() + "}";
      return value.getBytes();
    }

    @Override
    public String getTargetTopic(Tuple2 element) {
      // use always the default topic
      return null;
    }
  }

  public static void main(String[] args) throws Exception {


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // use filesystem based state management
    env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
    // checkpoint works fine if Flink is crashing but does not seem to work if job is restarted?
    env.enableCheckpointing(1000);

    Properties props = new Properties();
    props.setProperty("zookeeper.connect", "localhost:2181");
    props.setProperty("bootstrap.servers", "localhost:9092");
    // not to be shared with another job consuming the same topic
    props.setProperty("group.id", "flink-group");

    FlinkKafkaConsumer011<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer011<>(
            "origin",
            // true means we include metadata like topic name not
            // necessarily useful for this very example
            new JSONKeyValueDeserializationSchema(true),
            props);

    DataStream<ObjectNode> stream = env.addSource(kafkaConsumer);

    final SingleOutputStreamOperator<Tuple2> process =
            stream.keyBy(value -> value.get("key").asText())
            .window(GlobalWindows.create())
            // our trigger should probably be smarter
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
            .aggregate(new Average());

    FlinkKafkaProducer011 kafkaProducer = new FlinkKafkaProducer011<>(
            "localhost:9092",
            "flink-destination",
            new AverageSerializer());
    process.addSink(kafkaProducer);

    env.execute("FlinkAvgJob");
  }
}
