package com.example.flink.sessionwindow;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Instant;

public class App {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // SET TO EVENT TIME
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final Integer port = Integer.parseInt(params.get("port", "9090"));
        final String host = params.get("host");
        final String output = params.get("output");

        DataStream<String> data = env.socketTextStream(host, port);
        DataStream<Tuple3<Timestamp, Integer, Integer>> parsedData = data.map(new MapFunction<String, Tuple3<Timestamp, Integer, Integer>>() {
            @Override
            public Tuple3<Timestamp, Integer, Integer> map(String value) throws Exception {
                final String[] split = value.split(",");
                return new Tuple3<>(Timestamp.valueOf(split[0]), Integer.parseInt(split[1]), 0);
            }
        });

        DataStream<Tuple3<Timestamp, Integer, Integer>> summed = parsedData.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Timestamp, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Timestamp, Integer, Integer> element) {
                return element.f0.getTime();
            }
        }).windowAll(EventTimeSessionWindows.withGap(Time.seconds(3)))
                .reduce(new ReduceFunction<Tuple3<Timestamp, Integer, Integer>>() {
                    @Override
                    public Tuple3<Timestamp,Integer,Integer> reduce(Tuple3<Timestamp,Integer,Integer> accumulator, Tuple3<Timestamp, Integer, Integer> value) throws Exception {
                        return new Tuple3<>(Timestamp.from(Instant.now()), accumulator.f1 + value.f1, ++accumulator.f2);
                    }
                });

        summed.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        env.execute("Session window job");
    }
}
