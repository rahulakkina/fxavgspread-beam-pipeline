package ai.aliz.common;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Holds all constants needed for the job
 */
public interface FxRateConstants {
    int WINDOW_DURATION = 10;
    int WINDOW_SLIDE_EVERY = 1;
    Integer SC_NOT_FOUND = 404;

    String COMMA = ",";
    String HYPEN = "-";
    String REQUIRED_CURRENCY = "GBP/USD";

    //BigQuery table output
    String OUTPUT_FIELD[] = {"venue", "currency", "average_bid_rate", "average_ask_rate", "timestamp"};
    String OUTPUT_FIELD_DATA_TYPE[] = {"STRING", "STRING", "FLOAT", "FLOAT", "TIMESTAMP"};

    DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
    DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
}
