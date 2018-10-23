package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class VehicleTelematics {
    private static final Long MAX_SPEED = 90L;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String inFilePath = args[0];
        final String outFilePath = args[1];

        final DataStreamSource<String> source = env.readTextFile(inFilePath);
        source
                .map((MapFunction<String, VehicleReport>) in -> {
                    final Long[] params = Arrays.stream(in.split(","))
                            .map(Long::parseLong)
                            .toArray(Long[]::new);
                    return new VehicleReport(params);
                })
                .filter((FilterFunction<VehicleReport>) report -> report.getSpeed() > MAX_SPEED)
                .map((MapFunction<VehicleReport, String>) VehicleReport::speedFineOutputFormat)
                .writeAsText(outFilePath, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("SpeedRadar");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
