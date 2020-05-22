import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Objects.requireNonNull;

/**
 * An individual sensor reading, describing sensor id, sensor group id, reading, and timestamp.
 */
public class SensorReading {

    /** id of the sensor group */
    private String sensorGroup;

    /** id of the sensor */
    private String sensorId;

    /** The reading value */
    private double reading;

    /** the timestamp of the reading */
    private long timestamp;


    public SensorReading() {
        this("", "", 0L, 0.0);
    }

    public SensorReading(String sensorGroup, String sensorId, long timestamp, double reading) {
        this.sensorGroup = requireNonNull(sensorGroup);
        this.sensorId = requireNonNull(sensorId);
        this.timestamp = timestamp;
        this.reading = reading;
    }


    /**
     * Gets the ID of the sensor group.
     */
    public String sensorGroup() {
        return sensorGroup;
    }

    /**
     * Gets the ID of the sensor.
     */
    public String sensorId() {
        return sensorId;
    }

    /**
     * Gets the timestamp of the reading.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Gets the reading value.
     */
    public double reading() {
        return reading;
    }

    @Override
    public String toString() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS yy-MM-dd");
        String date = format.format(new Date(timestamp));

        return '(' + sensorId + '/' + sensorGroup + ") @ " + date + " : " + reading;
    }
}