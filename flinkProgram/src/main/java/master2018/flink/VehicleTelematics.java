package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final String inFilePath = args[0];
        final String outDirectoryPath = args[1];

        final SingleOutputStreamOperator<VehicleReport> vehicleReports = env.readTextFile(inFilePath).setParallelism(1)
                .map(VehicleTelematics::cellSplitter).setParallelism(1);

        // SpeedRadar
        vehicleReports
                .filter(report -> report.getSpeed() > MAX_SPEED)
                .map(VehicleTelematics::toSpeedFineFormat)
                .writeAsText(outDirectoryPath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // AccidentReporter
        vehicleReports
                .filter(report -> report.getSpeed() == 0).setParallelism(1)
                .keyBy(VehicleTelematics::keyByVehicleIdDirectionAndPosition)
                .countWindow(MAX_EVENTS, 1)
                .apply(VehicleTelematics::reportAccidents)
                .writeAsCsv(outDirectoryPath + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // AverageSpeedFine
        vehicleReports
                .filter(report -> 52 <= report.getSegment() && report.getSegment() <= 56)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<VehicleReport>() {
                    @Override
                    public long extractAscendingTimestamp(VehicleReport vehicleReport) {
                        return vehicleReport.getTimestamp();
                    }
                })
                .keyBy(VehicleTelematics::keyByVehicleIdHighwayIdAndDirection)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(31)))
                .apply(VehicleTelematics::calcAverageSpeed)
                .writeAsCsv(outDirectoryPath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static VehicleReport cellSplitter(String line) {
        String[] cells = line.split(",");
        return new VehicleReport(Long.parseLong(cells[0]), Long.parseLong(cells[1]),
                Integer.parseInt(cells[2]), Integer.parseInt(cells[3]), Integer.parseInt(cells[5]),
                Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
    }

    private static Tuple3<Long, Integer, Integer> keyByVehicleIdDirectionAndPosition(final VehicleReport vehicleReport) {
        return Tuple3.of(vehicleReport.getVehicleId(), vehicleReport.getDirection(), vehicleReport.getPosition());
    }

    private static void reportAccidents(final Tuple3<Long, Integer, Integer> key, final GlobalWindow window, final Iterable<VehicleReport> input, Collector<Tuple7<Long, Long, Long, Integer, Integer, Integer, Integer>> out) {
        if (Iterables.size(input) >= 4) {

            VehicleReport firstEvent = null;
            VehicleReport fourthEvent = null;
            Iterator<VehicleReport> iterator = input.iterator();

            int counter = 1;
            while (iterator.hasNext()) {
                if (counter == 1) {
                    firstEvent = iterator.next();
                    counter = counter + 1;
                } else if (counter == 4) {
                    fourthEvent = iterator.next();
                    counter = 1;
                } else {
                    iterator.next();
                    counter = counter + 1;
                }

                if (firstEvent != null && fourthEvent != null && firstEvent.getTimestamp() + 90 == fourthEvent.getTimestamp()) {
                    out.collect(new Tuple7<>(firstEvent.getTimestamp(), fourthEvent.getTimestamp(),
                            firstEvent.getVehicleId(), firstEvent.getHighwayId(), firstEvent.getSegment(),
                            firstEvent.getDirection(), firstEvent.getPosition()));
                    firstEvent = null;
                    fourthEvent = null;
                }
            }
        }
    }

    private static Tuple3<Long, Integer, Integer> keyByVehicleIdHighwayIdAndDirection(final VehicleReport vehicleReport) {
        return Tuple3.of(vehicleReport.getVehicleId(), vehicleReport.getHighwayId(), vehicleReport.getDirection());
    }

    private static void calcAverageSpeed(final Tuple3<Long, Integer, Integer> key, final TimeWindow window, final Iterable<VehicleReport> input, Collector<Tuple6<Long, Long, Long, Integer, Integer, Double>> out) {
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
        if (averageSpeed > MAX_AVG_SPEED) {
            out.collect(new AverageSpeedReport(startTime, endTime, key.getField(0), key.getField(1), key.getField(2), averageSpeed));
        }
    }

    private static String toSpeedFineFormat(final VehicleReport report) {
        return String.format("%s,%s,%s,%s,%s,%s", report.getTimestamp(), report.getVehicleId(), report.getHighwayId(), report.getSegment(), report.getDirection(), report.getSpeed());
    }
}
