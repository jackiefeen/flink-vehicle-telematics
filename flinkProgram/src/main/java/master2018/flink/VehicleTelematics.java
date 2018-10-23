package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple6<Long, Long, Long, Long, Long, Long>> filterOut =
                source.map(new MapFunction<String, Tuple6<Long, Long, Long, Long, Long, Long >>() {

                    @Override
                    public Tuple6<Long, Long, Long, Long, Long, Long> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple6<Long, Long, Long, Long, Long, Long> out =
                                new Tuple6<Long, Long, Long, Long, Long, Long>(Long.parseLong(fieldArray[0]),
                                        Long.parseLong(fieldArray[1]),
                                        Long.parseLong(fieldArray[3]), Long.parseLong(fieldArray[6]),
                                        Long.parseLong(fieldArray[5]), Long.parseLong(fieldArray[2]));
                        return out;
                    }

                }).filter(new FilterFunction<Tuple6<Long, Long, Long, Long, Long, Long>>() {
                    @Override
                    public boolean filter(Tuple6<Long, Long, Long, Long, Long, Long> in) throws Exception {
                        if (in.f5>90) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });
        filterOut.writeAsCsv(outFilePath);


        try {
            env.execute("SpeedRadar");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

