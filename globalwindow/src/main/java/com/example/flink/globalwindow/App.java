package com.example.flink.globalwindow;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;


public class App {
  final static String HOST = "localhost";
  final static Integer PORT = 9090;
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final ParameterTool params = ParameterTool.fromArgs(args);
    env.getConfig().setGlobalJobParameters(params);

    final String output = params.get("output");

    DataStream<Tuple3<Timestamp, Integer, Integer>> data = env.socketTextStream(HOST, PORT)
        .map(new MapFunction<String, Tuple3<Timestamp, Integer, Integer>>() {
          @Override
          public Tuple3<Timestamp, Integer, Integer> map(String s) throws Exception {
            final String[] vals = s.split(",");
            return new Tuple3<>(Timestamp.valueOf(vals[0]), Integer.parseInt(vals[1]), 0);
          }
        });
    DataStream<Tuple3<Timestamp, Integer, Integer>> parsed = data.keyBy(0)
        .window(GlobalWindows.create())
        .trigger(CountTrigger.of(10))
        .reduce(new ReduceFunction<Tuple3<Timestamp, Integer, Integer>>() {
          @Override
          public Tuple3<Timestamp, Integer, Integer> reduce(
              Tuple3<Timestamp, Integer, Integer> t1, Tuple3<Timestamp, Integer, Integer> t2)
              throws Exception {
            return new Tuple3<>(t1.f0, t1.f1 + t2.f1, ++t1.f2);
          }
        });

    parsed.writeAsCsv(output);
    env.execute("Global window job");
  }
}
