package master2018.flink;


class OutputFormatter {
    static String toSpeedFineFormat(final VehicleReport report) {
        return String.format("%s,%s,%s,%s,%s,%s", report.getTimestamp(), report.getVehicleId(), report.getHighwayId(), report.getSegment(), report.getDirection(), report.getSpeed());
    }

    static String toAvgSpeedFineFormat(final AverageSpeedReport report) {
        return String.format("%s,%s,%s,%s,%s,%s", report.getStartTime(), report.getEndTime(), report.getVehicleId(), report.getHighwayId(), report.getDirection(), report.getAverageSpeed());
    }
}

