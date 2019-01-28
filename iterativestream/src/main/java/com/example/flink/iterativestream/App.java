package com.example.flink.iterativestream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    static final String OUTPUT = "/home/scott/projects/udemy-flink-tutorial/iterativestream/output";
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<Long, Integer>> dataStream = env.generateSequence(0,5)
                .map(x -> new Tuple2<Long,Integer>(x, 0))
                .returns(new TypeHint<Tuple2<Long, Integer>>() {}); // 5000 is time to wait in millis

        IterativeStream<Tuple2<Long, Integer>> iterativeStream = dataStream.iterate(5000);
        DataStream<Tuple2<Long, Integer>> plusOne = iterativeStream
                .map(x -> {
                    if(x.f0 >= 10) {
                        return x;
                    } else {
                        return new Tuple2<Long, Integer>(++x.f0, ++x.f1);
                    }
                })
                .returns(new TypeHint<Tuple2<Long,Integer>>() {});

        DataStream<Tuple2<Long,Integer>> not10Yet = plusOne.filter(x -> x.f0 < 10);
        iterativeStream.closeWith(not10Yet);
        DataStream<Tuple2<Long,Integer>> already10 = plusOne.filter(x -> x.f0 >= 10);

        already10.writeAsText(OUTPUT);


        env.execute("Iterative stream");
    }
}
