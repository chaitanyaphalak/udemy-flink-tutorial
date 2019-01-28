package com.example.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;


/**
 * Hello world!
 *
 */
public class App 
{
    // Inner join
    // public static void main( String[] args ) throws Exception
    // {
    //     final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    //     final ParameterTool params = ParameterTool.fromArgs(args);
    //     env.getConfig().setGlobalJobParameters(params);

    //     // Read person file and generate tuple of person and id
    //     DataSet<Tuple2<Integer,String>> personSet = env.readTextFile(params.get("input1"))
    //                 .map(x -> {
    //                     final String[] words = x.split(",");
    //                     return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
    //                 }).returns(new TypeHint<Tuple2<Integer,String>>() {});

    //     // Read location file and generate tuple of location and id
    //     DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
    //                 .map(val -> {
    //                     final String[] words = val.split(",");
    //                     return new Tuple2<Integer,String>(Integer.parseInt(words[0]), words[1]);
    //                 }).returns(new TypeHint<Tuple2<Integer,String>>() {});
        
    //     // Join the datasets on the ids
    //     DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0).equalTo(0)
    //                 .with((x,y) -> new Tuple3<Integer, String, String>(x.f0, x.f1, y.f1))
    //                 .returns(new TypeHint<Tuple3<Integer,String,String>>(){});
        
    //     joined.writeAsCsv(params.get("output"), "\n", " ");
    //     env.execute("Join task");
    // }

    // left outer join
    // Same idea for right outer join too
    // public static void main( String[] args ) throws Exception
    // {
    //     final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    //     final ParameterTool params = ParameterTool.fromArgs(args);
    //     env.getConfig().setGlobalJobParameters(params);

    //     // Read person file and generate tuple of person and id
    //     DataSet<Tuple2<Integer,String>> personSet = env.readTextFile(params.get("input1"))
    //                 .map(x -> {
    //                     final String[] words = x.split(",");
    //                     return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
    //                 }).returns(new TypeHint<Tuple2<Integer,String>>() {});

    //     // Read location file and generate tuple of location and id
    //     DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
    //                 .map(val -> {
    //                     final String[] words = val.split(",");
    //                     return new Tuple2<Integer,String>(Integer.parseInt(words[0]), words[1]);
    //                 }).returns(new TypeHint<Tuple2<Integer,String>>() {});
        
    //     // Join the datasets on the ids
    //     DataSet<Tuple3<Integer, String, String>> joined = personSet.leftOuterJoin(locationSet).where(0).equalTo(0)
    //                 .with((x,y) -> new Tuple3<Integer, String, String>(x.f0, x.f1, y != null ? y.f1 : "NULL")) // NOte that y.f1 COULD be null here so have to check
    //                 .returns(new TypeHint<Tuple3<Integer,String,String>>(){});
        
    //     joined.writeAsCsv(params.get("output"), "\n", " ");
    //     env.execute("Join task");
    // }

    // Full join
    public static void main( String[] args ) throws Exception
    {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generate tuple of person and id
        DataSet<Tuple2<Integer,String>> personSet = env.readTextFile(params.get("input1"))
                    .map(x -> {
                        final String[] words = x.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }).returns(new TypeHint<Tuple2<Integer,String>>() {});

        // Read location file and generate tuple of location and id
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2"))
                    .map(val -> {
                        final String[] words = val.split(",");
                        return new Tuple2<Integer,String>(Integer.parseInt(words[0]), words[1]);
                    }).returns(new TypeHint<Tuple2<Integer,String>>() {});
        
        // Join the datasets on the ids
        DataSet<Tuple3<Integer, String, String>> joined = personSet.fullOuterJoin(locationSet).where(0).equalTo(0)
                    .with((x,y) -> new Tuple3<Integer, String, String>(x != null ? x.f0 : y.f0, x != null ? x.f1 : "NULL", y != null ? y.f1 : "NULL")) // NOte that y.f1 COULD be null here so have to check
                    .returns(new TypeHint<Tuple3<Integer,String,String>>(){});
        
        joined.writeAsCsv(params.get("output"), "\n", " ", FileSystem.WriteMode.OVERWRITE);
        env.execute("Join task");
    }
}
