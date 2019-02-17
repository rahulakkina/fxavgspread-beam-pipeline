package ai.aliz.task;

import ai.aliz.dto.FxRateInfo;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;

import static ai.aliz.common.FxRateConstants.HYPEN;
import static ai.aliz.common.FxRateConstants.OUTPUT_FIELD;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Does a transform and Computes rolling average and Writes a table row in BigQuery table
 */
public class FxRateAverage extends PTransform<PCollection<KV<String, FxRateInfo>>, PCollection<TableRow>> {
    private static final long serialVersionUID = 1L;

    /**
     * @param fxRateInfo
     * @return
     */
    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, FxRateInfo>> fxRateInfo) {

        final PCollection<KV<String, Iterable<FxRateInfo>>> timeGrouping = fxRateInfo.apply(GroupByKey.create());

        // Computes the average
        final PCollection<KV<String, FxRateInfo>> groupingStats = timeGrouping.apply(
                ParDo.of(
                        new DoFn<KV<String, Iterable<FxRateInfo>>, KV<String, FxRateInfo>>() {

                            private static final long serialVersionUID = 1L;

                            /**
                             *
                             * @param c
                             * @throws IOException
                             */
                            @ProcessElement
                            public void computeAverage(final ProcessContext c) throws IOException {

                                final String key[] = c.element().getKey().split(HYPEN);
                                final String venue = key[0];
                                final String currency = key[1];

                                final List<FxRateInfo> fxRateList = Lists.newArrayList(c.element().getValue());

                                final Float bidAvgVal =
                                        (float) fxRateList.parallelStream()
                                                .filter(item -> (item.getBidValue() != null))
                                                .mapToDouble(item -> (double) item.getBidValue())
                                                .average().getAsDouble();

                                final Float askAvgVal = 
                                         (float) fxRateList.parallelStream()
                                                 .filter(item -> (item.getAskValue() != null))
                                                 .mapToDouble(item -> (double) item.getAskValue())
                                                 .average().getAsDouble();

                                c.output(KV.of(venue,
                                        new FxRateInfo(venue, currency, bidAvgVal, askAvgVal)
                                ));
                            }
                        }
                )
        );

        //Builds a Table Row to persist on BigQuery
        final PCollection<TableRow> results = groupingStats.apply(
                ParDo.of(
                        new DoFn<KV<String, FxRateInfo>, TableRow>() {
                            private static final long serialVersionUID = 1L;

                            /**
                             *
                             * @param c
                             */
                            @ProcessElement
                            public void buildTableRow(ProcessContext c) {
                                final FxRateInfo fxRateInfo = c.element().getValue();
                                c.output(new TableRow()
                                        .set(OUTPUT_FIELD[0], fxRateInfo.getVenue())
                                        .set(OUTPUT_FIELD[1], fxRateInfo.getCurrency())
                                        .set(OUTPUT_FIELD[2], fxRateInfo.getBidValue())
                                        .set(OUTPUT_FIELD[3], fxRateInfo.getAskValue())
                                        .set(OUTPUT_FIELD[4], c.timestamp().toString()));
                            }
                        }
                )
        );

        return results;
    }
}
