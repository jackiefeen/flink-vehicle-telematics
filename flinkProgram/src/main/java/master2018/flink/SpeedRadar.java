/*
package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SpeedRadar {
    private static final int MAX_SPEED = 90;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String inFilePath = args[0];
        final String outFilePath = args[1];

        final DataStreamSource<String> source = env.readTextFile(inFilePath);

        source
                .map(line -> { String[] cells = line.split(",");
                    return new VehicleReport(Long.parseLong(cells[0]), Long.parseLong(cells[1]),
                            Integer.parseInt(cells[2]), Integer.parseInt(cells[3]), Integer.parseInt(cells[5]),
                            Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
                }).setParallelism(10)

                .filter((FilterFunction<VehicleReport>) report -> report.getSpeed() > MAX_SPEED).setParallelism(10)
                .map((MapFunction<VehicleReport, String>) VehicleReport::speedFineOutputFormat).setParallelism(10)
                .writeAsText(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("SpeedRadar");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


*/