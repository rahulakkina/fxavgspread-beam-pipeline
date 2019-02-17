
package ai.aliz.driver;

import ai.aliz.common.FxRateJobUtils;
import ai.aliz.dto.FxRateAverageSpreadOptions;
import ai.aliz.task.ExtractFxRates;
import ai.aliz.task.FxRateAverage;
import ai.aliz.task.ReadFxFileAndExtractDateTime;
import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.io.IOException;

import static ai.aliz.common.FxRateUtils.getSchema;


/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Actual driver which is responsible Pipeline trigger
 */
public class FxRateAverageSpreadDriver {

    /**
     * Sets up and starts streaming pipeline.
     *
     * @throws IOException if there is a problem setting up resources
     */
    public static void main(final String[] args) throws IOException {
        new FxRateAverageSpreadDriver().runFXRate(PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(FxRateAverageSpreadOptions.class));
    }

    public void runFXRate(final FxRateAverageSpreadOptions options) throws IOException {

        //Sets parameter to direct output of the pipeline underlying BigQuery table
        options.setBigQuerySchema(getSchema());

        final FxRateJobUtils fxRateJobUtils = new FxRateJobUtils(options);
        fxRateJobUtils.setup();

        final Pipeline pipeline = Pipeline.create(options);
        final TableReference tableRef = new TableReference();

        tableRef.setProjectId(options.getProject());
        tableRef.setDatasetId(options.getBigQueryDataset());
        tableRef.setTableId(options.getBigQueryTable());

        pipeline
                .apply("Reading Lines", new ReadFxFileAndExtractDateTime(options.getInputFile()))
                .apply(ParDo.of(new ExtractFxRates()))
                // map the incoming data stream into sliding windows.
                .apply(
                        Window.into(
                                //Defaults to Standard duration 10 mins as stated in the requirements
                                SlidingWindows.of(Duration.standardMinutes(options.getWindowDuration()))
                                        //Default Slide duration of 1 min
                                        .every(Duration.standardMinutes(options.getWindowSlideEvery()))))
                .apply(new FxRateAverage())
                .apply(BigQueryIO.writeTableRows()
                        .to(tableRef)
                        .withSchema(getSchema()));

        // Run the pipeline.
        final PipelineResult result = pipeline.run();

        // FxRateJobUtils will try to cancel the pipeline and the injector before the
        // program exists.
        fxRateJobUtils.waitToFinish(result);
    }


}
