package master2018.flink;

import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple6;

@NoArgsConstructor
public class AverageSpeedReport extends Tuple6<Long, Long, Long, Integer, Integer, Double> {

    public AverageSpeedReport(final Long startTime, final Long endTime, final Long vehicleId, final Integer highwayId,
                              final Integer direction, final Double averageSpeed) {
        this.f0 = startTime;
        this.f1 = endTime;
        this.f2 = vehicleId;
        this.f3 = highwayId;
        this.f4 = direction;
        this.f5 = averageSpeed;
    }

    public Long getStartTime() {
        return this.f0;
    }

    public Long getEndTime() {
        return this.f1;
    }

    public Long getVehicleId() {
        return this.f2;
    }

    public Integer getHighwayId() {
        return this.f3;
    }

    public Integer getDirection() {
        return this.f4;
    }

    public Double getAverageSpeed() {
        return this.f5;
    }
}
