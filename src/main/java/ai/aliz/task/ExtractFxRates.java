package ai.aliz.task;

import ai.aliz.dto.FxRateInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static ai.aliz.common.FxRateConstants.COMMA;
import static ai.aliz.common.FxRateConstants.REQUIRED_CURRENCY;
import static ai.aliz.common.FxRateUtils.*;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Extract each record and maps it with key VENUE-CURRNCY and wraps other fields in the DTO FxRateInfo
 */
public class ExtractFxRates extends DoFn<String, KV<String, FxRateInfo>> {

    private static final long serialVersionUID = 1L;

    /**
     * @param c
     */
    @ProcessElement
    public void extractRatesRecord(final ProcessContext processContext) {
        final String[] items = processContext.element().split(COMMA);
        final String venue = parseVenue(items);
        final String currency = parseCurrency(items);
        if (currency.equalsIgnoreCase(REQUIRED_CURRENCY)) {
            processContext.output(KV.of(
                    String.format("%s-%s", venue, currency),
                    new FxRateInfo(venue, currency,
                            parseBidPrice(items),
                            parseAskPrice(items))
            ));
        }

    }
}
