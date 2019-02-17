package ai.aliz.task;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

import static ai.aliz.common.FxRateConstants.COMMA;
import static ai.aliz.common.FxRateConstants.DATE_TIME_FORMATTER;
import static ai.aliz.common.FxRateUtils.parseTimestamp;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Extract the time stamp field from the input string, and use it as the element
 * time stamp.
 */
public class ExtractFxDateTime extends DoFn<String, String> {

    private static final long serialVersionUID = 1L;

    /**
     * @param processContext
     * @throws Exception
     */
    @ProcessElement
    public void extractTimeStamp(final ProcessContext processContext) throws Exception {
        final String[] items = processContext.element().split(COMMA);
        final String timestamp = parseTimestamp(items);

        if (timestamp != null) {
            try {
                processContext.outputWithTimestamp(processContext.element(), 
                                                   new Instant(DATE_TIME_FORMATTER.parseMillis(timestamp)));
            } catch (IllegalArgumentException e) {
                // Skip the invalid input.
            }
        }
    }
}
