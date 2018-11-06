/*
package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentReporter {

    private static final Integer MAX_EVENTS = 4;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final String inFilePath = args[0];
        final String outFilePath = args[1];

        final DataStreamSource<String> source = env.readTextFile(inFilePath);

        source.setParallelism(1)
                .map(line -> { String[] cells = line.split(",");
                    return new VehicleReport(Long.parseLong(cells[0]), Long.parseLong(cells[1]), Integer.parseInt(cells[2]),
                            Integer.parseInt(cells[3]), Integer.parseInt(cells[5]),
                            Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
                }).setParallelism(1)
                .filter((FilterFunction<VehicleReport>) report -> report.getSpeed() == 0).setParallelism(1)
                .keyBy((KeySelector<VehicleReport, Tuple3<Long, Integer, Integer>>) value ->
                        Tuple3.of(value.getVehicleId(), value.getDirection(), value.getPosition()))
                .countWindow(MAX_EVENTS, 1)
                .apply(new CustomWindowFunction()).setParallelism(10)
                .writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("AccidentReporter");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static class CustomWindowFunction implements WindowFunction<VehicleReport, Tuple7<Long, Long, Long, Integer, Integer, Integer, Integer>, Tuple3<Long, Integer, Integer>, GlobalWindow> {

        @Override
        public void apply(Tuple3<Long, Integer, Integer> key, GlobalWindow window, Iterable<VehicleReport> input, Collector<Tuple7<Long, Long, Long, Integer, Integer, Integer, Integer>> out){

            if (Iterables.size(input) >= 4) {

                VehicleReport firstevent = null;
                VehicleReport fourthevent = null;
                Iterator<VehicleReport> iterator = input.iterator();

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

                    if (firstevent != null && fourthevent != null && firstevent.getTimestamp()+90 == fourthevent.getTimestamp()) {
                        out.collect(new Tuple7<>(firstevent.getTimestamp(), fourthevent.getTimestamp(),
                                firstevent.getVehicleId(), firstevent.getHighwayId(), firstevent.getSegment(),
                                firstevent.getDirection(), firstevent.getPosition()));
                        firstevent = null;
                        fourthevent = null;
                    }
                }
            }

        }
    }
}


*/
