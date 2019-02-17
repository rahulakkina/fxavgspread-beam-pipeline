package ai.aliz.task;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * @author Rahul Akkina
 * @version v0.1
 */

/**
 * Reads the specified file and extract the timestamp
 */
public class ReadFileAndExtractDateTime extends PTransform<PBegin, PCollection<String>> {
    private static final long serialVersionUID = 1L;

    private final String inputFile;

    public ReadFileAndExtractDateTime(final String inputFile) {
        this.inputFile = inputFile;
    }

    @Override
    public PCollection<String> expand(final PBegin begin) {
        return begin.apply(TextIO.read().from(inputFile)).apply(ParDo.of(new ExtractFxDateTime()));
    }
}
