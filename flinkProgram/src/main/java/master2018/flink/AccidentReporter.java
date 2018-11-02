package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

public class AccidentReporter {

    private static final Long MAX_EVENTS = 4L;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final String inFilePath = args[0];
        final String outFilePath = args[1];

        final DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(10);
        //TODO: right now, the source cannot be parallelized - try to implement it to speed up execution?

        source.setParallelism(10)
                .map((MapFunction<String, VehicleReport>) in -> {
                    final Long[] params = Arrays.stream(in.split(","))
                            .map(Long::parseLong)
                            .toArray(Long[]::new);
                    return new VehicleReport(params);
                }).setParallelism(10)

                .keyBy((KeySelector<VehicleReport, Tuple3<Long, Long, Long>>) value ->
                Tuple3.of(value.getVehicleId(), value.getDirection(), value.getPosition()))
                .countWindow(MAX_EVENTS,1)
                .apply(new CustomWindowFunction()).setParallelism(10)
                .writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("AccidentReporter");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


private static class CustomWindowFunction implements WindowFunction<VehicleReport, Tuple7<Long, Long, Long, Long, Long, Long, Long>, Tuple3<Long, Long, Long>, GlobalWindow> {

    @Override
    public void apply(Tuple3<Long, Long, Long> key, GlobalWindow window, Iterable<VehicleReport> input, Collector<Tuple7<Long, Long, Long, Long, Long, Long, Long>> out) throws Exception {


        Iterator<VehicleReport> iterator = input.iterator();

        VehicleReport firstevent = null;
        VehicleReport fourthevent = null;


        int counter = 1;
        while (iterator.hasNext()) {
            if (counter == 1) {
                firstevent = iterator.next();
                counter = counter + 1;
            } else if (counter == 4) {
                fourthevent = iterator.next();
                counter = 1;
            } else {
                iterator.next();
                counter = counter + 1;
            }

            if (firstevent != null && fourthevent != null) {
                Long timestamp1 = firstevent.getTimestamp();
                Long timestamp2 = fourthevent.getTimestamp();

                out.collect(new Tuple7<>(timestamp1, timestamp2,
                        firstevent.getVehicleId(), firstevent.getHighwayId(), firstevent.getSegment(),
                        firstevent.getDirection(), firstevent.getPosition()));
                firstevent = null;
                fourthevent = null;
            }
        }
    }

}
}



