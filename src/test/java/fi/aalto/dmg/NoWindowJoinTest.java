package fi.aalto.dmg;

import fi.aalto.dmg.data.EventTimeData;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Created by jun on 20/11/15.
 */
public class NoWindowJoinTest implements Serializable{
    private static final long serialVersionUID = -4881298165222782534L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Window base on event time
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        final KeySelector<Tuple3<Long, String, Integer>, String> keySelector1 = new KeySelector<Tuple3<Long, String, Integer>, String>() {
            private static final long serialVersionUID = -2331085130954253915L;

            @Override
            public String getKey(Tuple3<Long, String, Integer> value) throws Exception {
                return value.f1;
            }
        };

        DataStream<Tuple3<Long, String, Integer>> age =
                env.fromCollection(EventTimeData.personWithAge)
                .assignTimestamps(new AscendingTimestampExtractor<Tuple3<Long, String, Integer>>() {
                    private static final long serialVersionUID = 7738552138432882737L;

                    @Override
                    public long extractAscendingTimestamp(Tuple3<Long, String, Integer> element, long currentTimestamp) {
                        return element.f0;
                    }
                });

        final KeySelector<Tuple3<Long, String, String>, String> keySelector2 = new KeySelector<Tuple3<Long, String, String>, String>() {
            private static final long serialVersionUID = -1496992413438410386L;

            @Override
            public String getKey(Tuple3<Long, String, String> value) throws Exception {
                return value.f1;
            }
        };

        DataStream<Tuple3<Long, String, String>> interest =
                env.fromCollection(EventTimeData.personWithInterest)
                .assignTimestamps(new AscendingTimestampExtractor<Tuple3<Long, String, String>>() {
                    private static final long serialVersionUID = 277502044775632740L;

                    @Override
                    public long extractAscendingTimestamp(Tuple3<Long, String, String> element, long currentTimestamp) {
                        return element.f0;
                    }
                });

        NoWindowJoinedStreams<Tuple3<Long, String, Integer>, Tuple3<Long, String, String>> joinedStreams =
                new NoWindowJoinedStreams<>(age, interest);
        DataStream<Tuple3<String, Integer, String>> newStream = joinedStreams.where(keySelector1)
                .buffer(Time.of(30, TimeUnit.SECONDS))
                .equalTo(keySelector2)
                .buffer(Time.of(30, TimeUnit.SECONDS))
                .apply(new JoinFunction<Tuple3<Long,String,Integer>, Tuple3<Long,String,String>, Tuple3<String, Integer, String>>() {
                    private static final long serialVersionUID = 2515265848293127260L;

                    @Override
                    public Tuple3<String, Integer, String> join(Tuple3<Long, String, Integer> first, Tuple3<Long, String, String> second) throws Exception {
                        if(!keySelector1.getKey(first).equals(keySelector2.getKey(second)))
                            throw new Exception("Two join items should have same key!");
                        return new Tuple3<>(first.f1, first.f2, second.f2);
                    }
                });
        newStream.print();

        env.execute("No Window Join");
    }

}
