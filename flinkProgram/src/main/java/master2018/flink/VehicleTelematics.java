package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class VehicleTelematics {
    private static final int MAX_SPEED = 90;

    @SuppressWarnings("serial")
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final String inFilePath = args[0];
        final String outFilePath = args[1];

        final DataStreamSource<String> source = env.readTextFile(inFilePath);

        //TODO: maybe the program runs more efficiently if we only ingest the fields that we need for the tasks in the, i.e. remove lane!
        //TODO: the bottleneck seems to be the very first map function!
        source
                .map(line -> { String[] cells = line.split(",");
                    return new VehicleReport(Integer.parseInt(cells[0]), Integer.parseInt(cells[1]), Integer.parseInt(cells[2]),
                            Integer.parseInt(cells[3]), Integer.parseInt(cells[4]), Integer.parseInt(cells[5]),
                            Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
                }).setParallelism(10)

                .filter((FilterFunction<VehicleReport>) report -> report.getSpeed() > MAX_SPEED).setParallelism(10)
                //Todo: this can be a lot more efficient without mapping it back to String. Let's use collect()!
                .map((MapFunction<VehicleReport, String>) VehicleReport::speedFineOutputFormat).setParallelism(10)
                .writeAsText(outFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("SpeedRadar");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


