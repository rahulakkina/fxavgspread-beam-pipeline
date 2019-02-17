package ai.aliz.dto;

import ai.aliz.common.FxRateBigQueryTableOptions;
import ai.aliz.common.FxRateOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

import static ai.aliz.common.FxRateConstants.WINDOW_DURATION;
import static ai.aliz.common.FxRateConstants.WINDOW_SLIDE_EVERY;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Sets FxRateAverageSpread job runtime options
 */
public interface FxRateAverageSpreadOptions extends FxRateOptions, FxRateBigQueryTableOptions {

    @Description("Path of the file to read from")
    String getInputFile();

    void setInputFile(String value);

    @Description("Numeric value of sliding window duration, in minutes")
    @Default.Integer(WINDOW_DURATION)
    Integer getWindowDuration();

    void setWindowDuration(Integer value);

    @Description("Numeric value of window 'slide every' setting, in minutes")
    @Default.Integer(WINDOW_SLIDE_EVERY)
    Integer getWindowSlideEvery();

    void setWindowSlideEvery(Integer value);
}
