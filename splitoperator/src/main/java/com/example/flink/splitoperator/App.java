package com.example.flink.splitoperator;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class App {

    static final String INPUT = "/home/scott/projects/udemy-flink-tutorial/splitoperator/oddeven";
    static final String OUTPUT = "/home/scott/projects/udemy-flink-tutorial/splitoperator/";
    static final String EVEN_LABEL = "even";
    static final String ODD_LABEL = "odd";
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> data = env.readTextFile(INPUT);
        SplitStream<String> split = data.split(x -> new ArrayList<String>() {{ add(Integer.parseInt(x) % 2 == 0 ? EVEN_LABEL : ODD_LABEL); }});
        split.select(EVEN_LABEL).writeAsText(OUTPUT + "even");
        split.select(ODD_LABEL).writeAsText(OUTPUT + "odd");
        env.execute();
    }
}
