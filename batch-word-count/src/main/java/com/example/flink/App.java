package com.example.flink;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        //** BASIC FLINK REQUIREMENTS FOR ALL FLINK JOBS */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        //************************************************/
        
        // Read file line by line based on input params (which is filepath)
        DataSet<String> text = env.readTextFile(params.get("input"));
        
        // Filter to find only lines that start with "n"
        DataSet<String> filtered = text.filter(x -> x.toLowerCase().startsWith("n"));
        

        // Map set to tokenizer (so map of name:count)
        DataSet<Tuple2<String, Integer>> tokenized = filtered
            .map(str -> new Tuple2<String, Integer>(str,Integer.valueOf(1)))
            .returns(new TypeHint<Tuple2<String,Integer>>(){});

        // Group by names (0th element of tuple) then sum the second element of tuple (1)
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
        
        // Create ouptut file
        if(params.has("output")) {
            // newline for each newline and fields separated by space
            counts.writeAsCsv(params.get("output"), "\n", " ");
            // execute program
            try {
                env.execute("Wordcount example");
            } catch(Exception exc) {
                System.out.println("Job execution threw exception: " + exc.getMessage());
                exc.printStackTrace();
            }
        } else {
            System.out.println("no output parameter");
            System.exit(0);
        }
    }
}
