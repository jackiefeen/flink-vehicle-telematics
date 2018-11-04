package master2018.flink;

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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class VehicleTelematics {
    private static final long NUMBER_OF_SEGMENTS = 5;
    private static final long MAX_SPEED = 90;
    private static final long MAX_AVG_SPEED = 60;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final String inFilePath = args[0];
        final String outDirectoryPath = args[1];
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final SingleOutputStreamOperator<VehicleReport> vehicleReports = env.readTextFile(inFilePath)
                .map(in -> {
                    final Long[] params = Arrays.stream(in.split(","))
                            .map(Long::parseLong)
                            .toArray(Long[]::new);
                    return new VehicleReport(params);
                });


        vehicleReports
                .filter(report -> report.getSpeed() > MAX_SPEED)
                .map(OutputFormatter::toSpeedFineFormat)
                .writeAsText(outDirectoryPath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE);

        vehicleReports
                .filter(report -> 52 <= report.getSegment() && report.getSegment() <= 56)
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
                .writeAsText(outDirectoryPath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE);

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

        Set<Long> coveredSegments = new HashSet<>();
        long startTime = Long.MAX_VALUE;
        long endTime = Long.MIN_VALUE;

        double aggregatedSpeed = 0;
        for (final VehicleReport report : input) {
            coveredSegments.add(report.getSegment());
            startTime = Long.min(startTime, report.getTimestamp());
            endTime = Long.max(endTime, report.getTimestamp());
            aggregatedSpeed += report.getSpeed();
        }

        if (coveredSegments.size() < NUMBER_OF_SEGMENTS) {
            return;
        }

        final double averageSpeed = aggregatedSpeed / numberOfReports;
        out.collect(new AverageSpeedReport(startTime, endTime, key.getField(0), key.getField(1), key.getField(2), averageSpeed));
    }

    private static Tuple3<Long, Long, Long> keyByVehicleIdHighwayIdAndDirection(final VehicleReport vehicleReport) {
        return Tuple3.of(vehicleReport.getVehicleId(), vehicleReport.getHighwayId(), vehicleReport.getDirection());
    }
}
