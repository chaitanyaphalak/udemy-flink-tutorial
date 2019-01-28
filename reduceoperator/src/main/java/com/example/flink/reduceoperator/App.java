package com.example.flink.reduceoperator;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {

    static final String INPUT_FILE = "/home/scott/projects/udemy-flink-tutorial/reduceoperator/avg";
    static final String OUTPUT= "/home/scott/projects/udemy-flink-tutorial/reduceoperator/";

    // Redure operator
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        env.getConfig().setGlobalJobParameters(parameterTool);
//        final String inputFile = parameterTool.get("input", INPUT_FILE);
//        DataStream<String> data = env.readTextFile(INPUT_FILE);
//        // map to month, product, category, profit, count
//        DataStream<Tuple2<String, Double>> averages = data.map(x -> {
//
//            final String[] segments = x.split(",");
//           // Split file on ',' and use everything execept the date. The 1 is for a count
//           return new Tuple5<String, String, String, Integer, Integer>(segments[1],
//                   segments[2],
//                   segments[3],
//                   Integer.parseInt(segments[4]), 1);
//        })
//        .returns(new TypeHint<Tuple5<String, String, String, Integer, Integer>>() {})
//        .keyBy(0)
//        .reduce((x,y) -> new Tuple5<>(x.f0, x.f1, x.f2, x.f3 + y.f3, x.f4 + y.f4))
//        .map(x -> new Tuple2<String, Double>(x.f0, new Double((x.f3 * 1.0)/x.f4)))
//        .returns(new TypeHint<Tuple2<String, Double>>(){});
//
//        averages.print();
//        env.execute("Average profits");
//
//    }

//    // Fold operation
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        env.getConfig().setGlobalJobParameters(parameterTool);
//        final String inputFile = parameterTool.get("input", INPUT_FILE);
//        DataStream<String> data = env.readTextFile(INPUT_FILE);
//        // map to month, product, category, profit, count
//        DataStream<Tuple2<String, Double>> averages = data.map(x -> {
//
//            final String[] segments = x.split(",");
//           // Split file on ',' and use everything execept the date. The 1 is for a count
//           return new Tuple5<String, String, String, Integer, Integer>(segments[1],
//                   segments[2],
//                   segments[3],
//                   Integer.parseInt(segments[4]), 1);
//        })
//        .returns(new TypeHint<Tuple5<String, String, String, Integer, Integer>>() {})
//        .keyBy(0)
//        .fold(new Tuple4<>("","",0,0),
//                (in, curr) -> {
//                    in.f0 = curr.f0;
//                    in.f1 = curr.f1;
//                    in.f2 += curr.f3;
//                    in.f3 += curr.f4;
//                    return in;
//                })
//        .returns(new TypeHint<Tuple4<String, String, Integer, Integer>>() {})
//        .map(x -> new Tuple2<String, Double>(x.f0, new Double((x.f2 * 1.0)/x.f3)))
//        .returns(new TypeHint<Tuple2<String, Double>>(){});
//
//        averages.print();
//        env.execute("Average profits");
//
//    }


    // Aggregation operations
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        final String inputFile = parameterTool.get("input", INPUT_FILE);
        DataStream<String> data = env.readTextFile(INPUT_FILE);
        // map to month, product, category, profit, count
        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(x -> {

            final String[] segments = x.split(",");
            // Split file on ',' and use everything execept the date. The 1 is for a count
            return new Tuple4<String, String, String, Integer>(segments[1],
                    segments[2],
                    segments[3],
                    Integer.parseInt(segments[4]));
        })
                .returns(new TypeHint<Tuple4<String, String, String, Integer>>() {});

        // Sum of monthly profits
        mapped.keyBy(0).sum(3).writeAsText(OUTPUT + "/sum");
        mapped.keyBy(0).min(3).writeAsText(OUTPUT + "/min");
        mapped.keyBy(0).minBy(3).writeAsText(OUTPUT + "/minBy");
        mapped.keyBy(0).max(3).writeAsText(OUTPUT + "/max");
        mapped.keyBy(0).maxBy(3).writeAsText(OUTPUT + "/maxBy");
        env.execute("Average profits");

    }
}
