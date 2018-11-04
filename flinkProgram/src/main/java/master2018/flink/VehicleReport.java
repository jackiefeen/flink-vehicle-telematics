package master2018.flink;

import lombok.Getter;

@Getter
class VehicleReport {
    private long timestamp;
    private long vehicleId;
    private long speed;
    private long highwayId;
    private long lane;
    private long direction;
    private long segment;
    private long position;

    VehicleReport(final Long... params) {
        if (params.length != 8) {
            throw new IllegalArgumentException();
        }

        this.timestamp = params[0];
        this.vehicleId = params[1];
        this.speed = params[2];
        this.highwayId = params[3];
        this.lane = params[4];
        this.direction = params[5];
        this.segment = params[6];
        this.position = params[7];
    }
}
