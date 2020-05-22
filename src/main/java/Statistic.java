import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Objects.requireNonNull;

/**
 * An aggregate of a sensor or group reading with a timestamp.
 */
public class Statistic {

    /** The group or sensor id */
    private String id;

    /** The timestamp of the aggregate */
    private long timestamp;

    /** The aggregate value */
    private double value;


    public Statistic() {
        this("", 0L, 0.0);
    }

    public Statistic(String id, long timestamp, double value) {
        this.id = requireNonNull(id);
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * Gets the statistic's sensor- or group id.
     */
    public String id() {
        return id;
    }

    /**
     * Gets the statistic's timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Gets the aggregate value.
     */
    public double value() {
        return value;
    }

    @Override
    public String toString() {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS yy-MM-dd");
        String date = format.format(new Date(timestamp));

        return id + " @ " + date + " : " + value;
    }
}