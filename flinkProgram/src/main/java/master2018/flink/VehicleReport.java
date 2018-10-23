package master2018.flink;

import lombok.Getter;

@Getter
class VehicleReport {
    private Long timestamp;
    private Long vehicleId;
    private Long speed;
    private Long highwayId;
    private Long lane;
    private Long direction;
    private Long segment;
    private Long position;

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

    String speedFineOutputFormat() {
        return String.format("%s,%s,%s,%s,%s,%s", timestamp, vehicleId, highwayId, segment, direction, speed);
    }
}
