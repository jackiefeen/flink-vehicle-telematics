package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class VehicleTelematics {

    private static final int MAX_SPEED = 90;
    private static final Integer MAX_EVENTS = 4;
    private static final long MAX_AVG_SPEED = 60;
    private static final long NUMBER_OF_SEGMENTS = 5;
    private static final double METERS_PER_SEC_TO_MILES_PER_HOUR = 2.23693629;

    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String inFilePath = args[0];
        final String outDirectoryPath = args[1];

        final DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<VehicleReport> speedradarmappeddata =
                source.map(VehicleTelematics::CellSplitter).startNewChain().setParallelism(10);

        //SpeedRadar
        speedradarmappeddata
                .filter((FilterFunction<VehicleReport>) report -> report.getSpeed() > MAX_SPEED).setParallelism(10)
                .map(VehicleReport::speedFineOutputFormat).setParallelism(10)
                .writeAsText(outDirectoryPath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        SingleOutputStreamOperator<VehicleReport> mappeddata = source.setParallelism(1)
                .map(VehicleTelematics::CellSplitter).startNewChain().setParallelism(1);

        //AccidentReporter
        mappeddata
                .filter((FilterFunction<VehicleReport>) report -> report.getSpeed() == 0).startNewChain().setParallelism(1)
                .keyBy(VehicleTelematics::keyByVehicleIdDirectionAndPosition)
                .countWindow(MAX_EVENTS, 1)
                .apply(new CustomWindowFunction()).setParallelism(10)
                .writeAsCsv(outDirectoryPath + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //AverageSpeedFine
        mappeddata
                .filter(report -> 52 <= report.getSegment() && report.getSegment() <= 56).startNewChain().setParallelism(1)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<VehicleReport>() {
                    @Override
                    public long extractAscendingTimestamp(VehicleReport vehicleReport) {
                        return vehicleReport.getTimestamp() * 1000;
                    }
                })
                .keyBy(VehicleTelematics::keyByVehicleIdHighwayIdAndDirection)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(VehicleTelematics::calcAverageSpeed)
                .filter(report -> report.getAverageSpeed() > MAX_AVG_SPEED)
                .map(OutputFormatter::toAvgSpeedFineFormat)
                .writeAsText(outDirectoryPath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static VehicleReport CellSplitter(String line) {
        String[] cells = line.split(",");
        return new VehicleReport(Long.parseLong(cells[0]), Long.parseLong(cells[1]),
                Integer.parseInt(cells[2]), Integer.parseInt(cells[3]), Integer.parseInt(cells[5]),
                Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
    }

    public static class CustomWindowFunction implements WindowFunction<VehicleReport, Tuple7<Long, Long, Long, Integer, Integer, Integer, Integer>, Tuple3<Long, Integer, Integer>, GlobalWindow> {

        @Override
        public void apply(Tuple3<Long, Integer, Integer> key, GlobalWindow window, Iterable<VehicleReport> input, Collector<Tuple7<Long, Long, Long, Integer, Integer, Integer, Integer>> out) {

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

                    if (firstevent != null && fourthevent != null && firstevent.getTimestamp() + 90 == fourthevent.getTimestamp()) {
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

    private static Tuple3<Long, Integer, Integer> keyByVehicleIdDirectionAndPosition(final VehicleReport vehicleReport) {
        return Tuple3.of(vehicleReport.getVehicleId(), vehicleReport.getDirection(), vehicleReport.getPosition());
    }


    private static void calcAverageSpeed(final Tuple key, final TimeWindow window, final Iterable<VehicleReport> input, Collector<AverageSpeedReport> out) {
        final int numberOfReports = Iterables.size(input);
        if (numberOfReports < NUMBER_OF_SEGMENTS) {
            return;
        }

        Set<Integer> coveredSegments = new HashSet<>();
        long startTime = Long.MAX_VALUE;
        long endTime = Long.MIN_VALUE;
        int minPosition = Integer.MAX_VALUE;
        int maxPosition = Integer.MIN_VALUE;
        for (final VehicleReport report : input) {
            coveredSegments.add(report.getSegment());
            startTime = Long.min(startTime, report.getTimestamp());
            endTime = Long.max(endTime, report.getTimestamp());
            minPosition = Integer.min(minPosition, report.getPosition());
            maxPosition = Integer.max(maxPosition, report.getPosition());
        }

        if (coveredSegments.size() < NUMBER_OF_SEGMENTS) {
            return;
        }

        final double positionDelta = maxPosition - minPosition;
        final double timeDelta = endTime - startTime;
        final double averageSpeed = positionDelta / timeDelta * METERS_PER_SEC_TO_MILES_PER_HOUR;
        out.collect(new AverageSpeedReport(startTime, endTime, key.getField(0), key.getField(1), key.getField(2), averageSpeed));
    }

    private static Tuple3<Long, Integer, Integer> keyByVehicleIdHighwayIdAndDirection(final VehicleReport vehicleReport) {
        return Tuple3.of(vehicleReport.getVehicleId(), vehicleReport.getHighwayId(), vehicleReport.getDirection());
    }


}

