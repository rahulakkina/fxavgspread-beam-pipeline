package ai.aliz.dto;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

/**
 * @author Rahul Akkina
 * @version v0.1
 */
@DefaultCoder(AvroCoder.class)
public class FxRateInfo implements Serializable {

    private static final long serialVersionUID = -7883452527722894189L;

    @Nullable
    final String venue;
    @Nullable
    final String currency;
    @Nullable
    final Float bidValue;
    @Nullable
    final Float askValue;

    public FxRateInfo(final String venue, final String currency, final Float bidValue, final Float askValue) {
        this.venue = venue;
        this.currency = currency;
        this.bidValue = bidValue;
        this.askValue = askValue;
    }

    public String getVenue() {
        return venue;
    }

    public String getCurrency() {
        return currency;
    }

    public Float getBidValue() {
        return bidValue;
    }

    public Float getAskValue() {
        return askValue;
    }
}
