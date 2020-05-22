import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class MyFlink {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.registerType(Statistic.class);
        env.registerType(SensorReading.class);

        //env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // create a stream of sensor readings, assign timestamps, and create watermarks
        DataStream<SensorReading> readings = env
                .addSource(new SampleDataGenerator())
                .assignTimestamps(new ReadingsTimestampAssigner());

        // path (1) - low latency event-at a time filter
        readings
                .filter(reading -> reading.reading() > 100.0)
                .map( reading -> "-- ALERT -- Reading above threshold: " + reading )
                .print();

        // path (2) - processing time windows: Compute max readings per sensor group

        // because the default stream time is set to Event Time, we override the trigger with a
        // processing time trigger

        readings
                .keyBy( reading -> reading.sensorGroup() )
                .window(TumblingTimeWindows.of(Time.seconds(5)))
                .trigger(ProcessingTimeTrigger.create())
                .fold(new Statistic(), (curr, next)->
                        new Statistic(next.sensorGroup(), next.timestamp(), Math.max(curr.value(), next.reading())))

                .map(stat -> "PROC TIME - max for " + stat)
                .print();

        // path (3) - event time windows: Compute average reading over sensors per minute

        // we use a WindowFunction here, to illustrate how to get access to the window object
        // that contains bounds, etc.
        // Pre-aggregation is possible by adding a pre-aggregator ReduceFunction

        readings
                // group by, window and aggregate
                .keyBy(reading -> reading.sensorId() )
                .timeWindow(Time.minutes(1), Time.seconds(10))
                .apply(new WindowFunction<SensorReading, Statistic, String, TimeWindow>() {

                    @Override
                    public void apply(String id, TimeWindow window, Iterable<SensorReading> values, Collector<Statistic> out) {
                        int count = 0;
                        double agg = 0.0;
                        for (SensorReading r : values) {
                            agg += r.reading();
                            count++;
                        }
                        out.collect(new Statistic(id, window.getStart(), agg / count));
                    }
                })

                .map(stat -> "EVENT TIME - avg for " + stat)
                .print();

        env.execute("Event time example");
    }

    private static class ReadingsTimestampAssigner implements TimestampExtractor<SensorReading> {

        private static final long MAX_DELAY = 12000;

        private long maxTimestamp;

        @Override
        public long extractTimestamp(SensorReading element, long currentTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.timestamp());
            return element.timestamp();
        }

        @Override
        public long extractWatermark(SensorReading element, long currentTimestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return maxTimestamp - MAX_DELAY;
        }

    }
}





























































































/*

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


// Ayto diabazei apo nc kai ta epe3ergazetai kai ta stelnei se arxeio.

public class MyFlink {



    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

     //   env.setParallelism(2);

        DataStream<Tuple2<Integer, Integer>> dataStream = env
                .socketTextStream("localhost", 8888)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.of(10, TimeUnit.SECONDS))
                .min(1);



       // dataStream.writeAsText("file:///home/ioakim/IdeaProjects/flink02/out/artifacts/flink02_jar/output");
        dataStream.print();
        env.execute("Window WordCount");

    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<Integer, Integer>(Integer.parseInt(word)+0, 1));
            }
        }
    }

}
*/
/////////////////////////////////////////////////
//////////////////////////////////////////////////
//////////////////////////////////////////////////
/////////////////////////////////////////////////

/*
    public static void main(String[] args) throws java.lang.Exception {

        String topic = "test";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //  env.enableCheckpointing();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));

        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>((topic), new SimpleStringSchema(), props));

        DataStream<Tuple2<String, Integer>> counts = messageStream.flatMap(new Splitter()).keyBy(0).timeWindow(Time.of(1, TimeUnit.SECONDS)).sum(1);


        //messageStream.print();
        counts.print();

        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) {
                //  int v = Integer.parseInt(value);
                //  int s = v + 2;
                //   System.out.println("Sum: " + s);
                return "Kafka and Flink says.: " + value;
            }
        });//.print();
        env.execute();
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
*/

