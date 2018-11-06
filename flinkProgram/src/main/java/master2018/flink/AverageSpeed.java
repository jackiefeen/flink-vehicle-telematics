/*
package master2018.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;


public class AverageSpeed {

    private static final long NUMBER_OF_SEGMENTS = 5;
    private static final long MAX_AVG_SPEED = 60;
    private static final double METERS_PER_SEC_TO_MILES_PER_HOUR = 2.23693629;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final String inFilePath = args[0];
        final String outDirectoryPath = args[1];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final SingleOutputStreamOperator<VehicleReport> vehicleReports = env.readTextFile(inFilePath).setParallelism(1)
                .map(line -> {
                    final String[] cells = line.split(",");
                    return new VehicleReport(Long.parseLong(cells[0]), Long.parseLong(cells[1]),
                            Integer.parseInt(cells[2]), Integer.parseInt(cells[3]), Integer.parseInt(cells[5]),
                            Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
                }).setParallelism(1);

        vehicleReports
                .filter(report -> 52 <= report.getSegment() && report.getSegment() <= 56).setParallelism(1)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<VehicleReport>() {
                    @Override
                    public long extractAscendingTimestamp(VehicleReport vehicleReport) {
                        return vehicleReport.getTimestamp() * 1000;
                    }
                })
                .keyBy(AverageSpeed::keyByVehicleIdHighwayIdAndDirection)
                .window(EventTimeSessionWindows.withGap(Time.seconds(31)))
                .apply(AverageSpeed::calcAverageSpeed)
                .filter(report -> report.getAverageSpeed() > MAX_AVG_SPEED)
                .map(OutputFormatter::toAvgSpeedFineFormat)
                .writeAsText(outDirectoryPath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
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

*/