package com.example.flink.cabassignment;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    static final String ONGOING = "yes";
    static final String ENDED = "no";
    static final String NULL = "'null'";

    // DataStream...does not work
//    public static void main(String[] args) throws Exception {
//
//        // TODO Fix this...it's not working right. Not sure what I'm doing wrong
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        env.getConfig().setGlobalJobParameters(params);
//
//        final String inputFile = params.get("input");
//        final String popDest = params.get("popularDest");
//        final String outputFile = params.get("output");
//        // cab id, cab type, cab number plate, cab driver name, ongoing trip/not, pickup, destination, passenger count
//        DataStream<Tuple8<String,String, String, String, String, String, String, Integer>> data = env.readTextFile(inputFile)
//                .map(x -> {
//                    final String[] vals = x.split(",");
//                    Integer passengerCnt;
//                    if (NULL.equals(vals[7])) {
//                        passengerCnt = 0;
//                    } else {
//                        passengerCnt = Integer.parseInt(vals[7]);
//                    }
//                    return new Tuple8<>(vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], passengerCnt);
//                })
//                .returns(new TypeHint<Tuple8<String, String, String, String, String, String, String, Integer>>() {})
//                .filter(x -> ONGOING.equals(x.f4)); // only want ongoing trips
//
////        DataStream<Tuple2<String,Integer>> folded =
//                data.keyBy(6)
//                .fold(new Tuple2<>("", 0), (out, in) -> {
//                    out.f0 = in.f6;
//                    out.f1 += in.f7;
//                    return out;
//                })
////                .fold(0, (out,in) -> out += in.f7)
//                .returns(new TypeHint<Tuple2<String, Integer>>(){})
//                .writeAsText(popDest, FileSystem.WriteMode.OVERWRITE);
//
//   //     folded.print();
////        folded.keyBy(0)
////                .maxBy(1)
////                .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE);
//        env.execute("Cab Job");
//    }

    // Batch processing, works fine
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        final String inputFile = params.get("input");
        final String popDest = params.get("popularDest");
        final String outputFile = params.get("output");

        DataSet<Tuple8<String,String, String, String, String, String, String, Integer>> data = env.readTextFile(inputFile)
        .map(x -> {
            final String[] vals = x.split(",");
            Integer passengerCnt;
            if (NULL.equals(vals[7])) {
                passengerCnt = 0;
            } else {
                passengerCnt = Integer.parseInt(vals[7]);
            }
            return new Tuple8<>(vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], passengerCnt);
        })
        .returns(new TypeHint<Tuple8<String, String, String, String, String, String, String, Integer>>() {})
        .filter(x -> ONGOING.equals(x.f4)); // only want ongoing trips

        // group by
        data.groupBy(6)
                .reduce((x,y) -> new Tuple8<>(x.f0, x.f1, x.f2, x.f3, x.f4, x.f5, x.f6, x.f7 + y.f7))
                .maxBy(7)
                .map(x -> x.f6 + " - " + x.f7)
                .returns(new TypeHint<String>(){})
                .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }
}
