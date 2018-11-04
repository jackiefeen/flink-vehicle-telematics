package master2018.flink;

class VehicleReport {
    private Integer timestamp;
    private Integer vehicleId;
    private Integer speed;
    private Integer highwayId;
    private Integer lane;
    private Integer direction;
    private Integer segment;
    private Integer position;


    VehicleReport(final Integer... params) {
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


    public Integer getTimestamp() {
        return timestamp;
    }

    public Integer getVehicleId() {
        return vehicleId;
    }

    public Integer getSpeed() {
        return speed;
    }

    public Integer getHighwayId() {
        return highwayId;
    }

    public Integer getLane() {
        return lane;
    }

    public Integer getDirection() {
        return direction;
    }

    public Integer getSegment() {
        return segment;
    }

    public Integer getPosition() {
        return position;
    }

    String speedFineOutputFormat() {
        return String.format("%s,%s,%s,%s,%s,%s", timestamp, vehicleId, highwayId, segment, direction, speed);
    }

}


