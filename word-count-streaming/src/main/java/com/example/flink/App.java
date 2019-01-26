package com.example.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Hello world!
 *
 */
public class App 
{
    static final String HOST = "localhost";
    static final int PORT = 9999;

    public static void main( String[] args ) throws Exception
    {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        final String host = params.get("host", HOST);
        final int port = Integer.parseInt(params.get("port", Integer.toString(PORT)));

        DataStream<String> text = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> counts = text
                .filter(x -> x.toLowerCase().startsWith("n"))
                .map(x -> new Tuple2<>(x, Integer.valueOf(1)))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(0) // groupBy first field of tuple
                .sum(1); // sum 2nd field of tuple

        counts.print();
        env.execute();
    }
}
