package com.example.flink.tumblingwindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;

public class App {
    static final String DEFAULT_PORT = "9090";
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        Integer port = Integer.parseInt(params.get("port", DEFAULT_PORT));
        final String ouput = params.get("output");

        DataStream<String> data = env.socketTextStream("localhost", port);
        DataStream<Tuple2<Timestamp, Integer>> sum = data.map(new MapFunction<String, Tuple2<Timestamp, Integer>>() {
                    public Tuple2<Timestamp, Integer> map(String value) throws Exception {
                        final String[] values = value.split(",");
                        return new Tuple2(Timestamp.valueOf(values[0]), Integer.parseInt(values[1]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Timestamp, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Timestamp, Integer> element) {
                        return element.f0.getTime();
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Timestamp, Integer>>() {
                    @Override
                    public Tuple2<Timestamp, Integer> reduce(Tuple2<Timestamp, Integer> value1, Tuple2<Timestamp, Integer> value2) throws Exception {
                        return new Tuple2<>(Timestamp.from(Instant.now()), value1.f1 + value2.f1);
                    }
                });
        sum.writeAsText(ouput, FileSystem.WriteMode.OVERWRITE);
        env.execute("Tumbling window job");
    }

}
