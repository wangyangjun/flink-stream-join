package fi.aalto.dmg;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.MultiWindowsJoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 03/12/15.
 */
public class KafkaWindowJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Window base on event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        final KeySelector<Tuple2<String, Long>, String> keySelector = new KeySelector<Tuple2<String, Long>, String>() {
            private static final long serialVersionUID = -1787574339917074648L;

            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        };

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "test");
        properties.put("topic", "advertisement");
        properties.put("auto.offset.reset", "smallest");

        TimestampExtractor<Tuple2<String, Long>> timestampExtractor1 = new AscendingTimestampExtractor<Tuple2<String, Long>>() {
            private static final long serialVersionUID = 8965896144592350020L;

            @Override
            public long extractAscendingTimestamp(Tuple2<String, Long> element, long currentTimestamp) {
                return element.f1;
            }
        };

        TimestampExtractor<Tuple2<String, Long>> timestampExtractor2 = new TimestampExtractor<Tuple2<String, Long>>() {
            private static final long serialVersionUID = -6672551198307157846L;
            private long currentTimestamp = 0;

            @Override
            public final long extractTimestamp(Tuple2<String, Long> element, long currentTimestamp) {
                long newTimestamp = element.f1;
                this.currentTimestamp = newTimestamp;
                return this.currentTimestamp;
            }

            @Override
            public final long extractWatermark(Tuple2<String, Long> element, long currentTimestamp) {
                return currentTimestamp-30000;
            }

            @Override
            public final long getCurrentWatermark() {
                return currentTimestamp - 30000;
            }
        };

        DataStream<Tuple2<String, Long>> advertisement = env
                .addSource(new FlinkKafkaConsumer082<String>("advertisement", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    private static final long serialVersionUID = -6564495005753073342L;

                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] splits = value.split(" ");
                        return new Tuple2<String, Long>(splits[0], Long.parseLong(splits[1]));
                    }
                }).keyBy(keySelector).assignTimestamps(timestampExtractor1);

        DataStream<Tuple2<String, Long>> click = env
                .addSource(new FlinkKafkaConsumer082<String>("click", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    private static final long serialVersionUID = -6564495005753073342L;

                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] splits = value.split(" ");
                        return new Tuple2<String, Long>(splits[0], Long.parseLong(splits[1]));
                    }
                }).keyBy(keySelector).assignTimestamps(timestampExtractor2);

        MultiWindowsJoinedStreams<Tuple2<String, Long>, Tuple2<String, Long>> joinedStreams =
                new MultiWindowsJoinedStreams<>(advertisement, click);

        DataStream<Tuple3<String, Long, Long>> joinedStream = joinedStreams.where(keySelector)
                .window(SlidingTimeWindows.of(Time.of(25, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)))
                .equalTo(keySelector)
                .window(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    private static final long serialVersionUID = -3625150954096822268L;

                    @Override
                    public Tuple3<String, Long, Long> join(Tuple2<String, Long> first, Tuple2<String, Long> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });

        joinedStream = joinedStream.filter(new FilterFunction<Tuple3<String, Long, Long>>() {
            private static final long serialVersionUID = -4325256210808325338L;

            @Override
            public boolean filter(Tuple3<String, Long, Long> value) throws Exception {
                return value.f1<value.f2&&value.f1+20000>=value.f2;
            }
        });

//        joinedStream.addSink(new SinkFunction<Tuple3<String, Long, Long>>() {
//            private static final long serialVersionUID = 5162922996056277977L;
//
//            @Override
//            public void invoke(Tuple3<String, Long, Long> value) throws Exception {
//                // write to mysql
//                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test?" +
//                                        "user=root&password=123456");
//                Statement stmt = null;
//
//                stmt = conn.createStatement();
//                stmt.execute("INSERT INTO advertise_shown_join"
//                        + " VALUES('" + value.f0 + "'," + String.valueOf(value.f1) + ","
//                        + String.valueOf(value.f2) +")");
//                conn.close();
//            }
//        });

        DataStream<Tuple2<String, Integer>> newStream = joinedStream.map(new MapFunction<Tuple3<String,Long,Long>, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -8619815378463068708L;

            @Override
            public Tuple2<String, Integer> map(Tuple3<String, Long, Long> value) throws Exception {
                return new Tuple2<String, Integer>("Key", 1);
            }
        }).keyBy(0).sum(1);
        newStream.print();


        env.execute("Window Join");

    }
}
