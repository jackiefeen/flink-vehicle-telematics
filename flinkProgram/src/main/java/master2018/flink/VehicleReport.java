package master2018.flink;

import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple7;

@NoArgsConstructor
public class VehicleReport extends Tuple7<Long, Long, Integer, Integer, Integer, Integer, Integer> {

    public VehicleReport(final Long timestamp, final Long vehicleId, final Integer speed,
                         final Integer highwayId, final Integer direction, final Integer segment, final Integer position) {
        this.f0 = timestamp;
        this.f1 = vehicleId;
        this.f2 = speed;
        this.f3 = highwayId;
        this.f4 = direction;
        this.f5 = segment;
        this.f6 = position;
    }

    public Long getTimestamp() {
        return this.f0;
    }

    public Long getVehicleId() {
        return this.f1;
    }

    public Integer getSpeed() {
        return this.f2;
    }

    public Integer getHighwayId() {
        return this.f3;
    }

    public Integer getDirection() {
        return this.f4;
    }

    public Integer getSegment() {
        return this.f5;
    }

    public Integer getPosition() {
        return this.f6;
    }
}
