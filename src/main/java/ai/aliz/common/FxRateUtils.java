package ai.aliz.common;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.aliz.common.FxRateConstants.OUTPUT_FIELD;
import static ai.aliz.common.FxRateConstants.OUTPUT_FIELD_DATA_TYPE;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Provides number of utility methods
 */
public abstract class FxRateUtils {

    /**
     * @param inputItems
     * @return
     */
    public static String parseVenue(final String[] inputItems) {
        return parseString(inputItems, 0);
    }

    /**
     * @param inputItems
     * @return
     */
    public static String parseCurrency(final String[] inputItems) {
        return parseString(inputItems, 1);
    }

    /**
     * @param inputItems
     * @return
     */
    public static String parseTimestamp(final String[] inputItems) {
        return parseString(inputItems, 2);
    }

    /**
     * @param inputItems
     * @return
     */
    public static Float parseBidPrice(final String[] inputItems) {
        try {
            return Float.parseFloat(parseString(inputItems, 3));
        } catch (NumberFormatException | NullPointerException e) {
            return null;
        }
    }

    /**
     * @param inputItems
     * @return
     */
    public static Float parseAskPrice(final String[] inputItems) {
        try {
            return Float.parseFloat(parseString(inputItems, 4));
        } catch (NumberFormatException | NullPointerException e) {
            return null;
        }
    }

    /**
     * @param inputItems
     * @param index
     * @return
     */
    public static String parseString(final String[] inputItems, final int index) {
        return (inputItems.length > index) ? inputItems[index] : null;
    }


    /**
     * ** Defines the BigQuery schema used for the output.
     */
    public static TableSchema getSchema() {
        return new TableSchema().setFields(
                IntStream.range(0, OUTPUT_FIELD.length)
                        //maps field to a TableFieldSchema object
                        .mapToObj(i -> new TableFieldSchema().setName(OUTPUT_FIELD[i]).setType(OUTPUT_FIELD_DATA_TYPE[i]))
                        .collect(Collectors.toList())
        );
    }
}
