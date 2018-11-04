package master2018.flink;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
class AverageSpeedReport {
    private long startTime;
    private long endTime;
    private long vehicleId;
    private long highwayId;
    private long direction;
    private double averageSpeed;
}
