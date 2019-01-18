package io.crate.execution.engine.reader;

import com.google.common.collect.ImmutableMap;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import io.crate.execution.engine.collect.files.LineCollectorExpression;
import io.crate.execution.engine.collect.files.LocalFsFileInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;
import static io.crate.testing.TestingHelpers.createReference;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class CsvReaderBenchmark {

    private String fileUri;
    private InputFactory inputFactory;
    private TransactionContext txnCtx = TransactionContext.of("dummyUser", SearchPath.createSearchPathFrom("dummySchema"));
    File tempFile;

    @Setup
    public void create_temp_file_and_uri() throws IOException {
        Functions functions = new Functions(
            ImmutableMap.of(),
            ImmutableMap.of()
        );
        inputFactory = new InputFactory(functions);
        tempFile = File.createTempFile("temp", null);
        fileUri = tempFile.toURI().getPath();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
            writer.write("name,id\n");
            writer.write("Arthur,4\n");
            writer.write("Trillian,5\n");
            writer.write("Emma,5\n");
            writer.write("Emily,9\n");
            writer.write("Sarah,5\n");
            writer.write("John,5\n");
            writer.write("Mical,9\n");
            writer.write("Mary,5\n");
            writer.write("Jimmy,9\n");
            writer.write("Tom,5\n");
            writer.write("Neil,0\n");
            writer.write("Rose,5\n");
            writer.write("Gobnait,5\n");
            writer.write("Rory,1\n");
            writer.write("Martin,11\n");
            writer.write("Arthur,4\n");
            writer.write("Trillian,5\n");
            writer.write("Emma,5\n");
            writer.write("Emily,9\n");
            writer.write("Sarah,5\n");
            writer.write("John,5\n");
            writer.write("Mical,9\n");
            writer.write("Mary,5\n");
            writer.write("Jimmy,9\n");
            writer.write("Tom,5\n");
            writer.write("Neil,0\n");
            writer.write("Rose,5\n");
            writer.write("Gobnait,5\n");
            writer.write("Rory,1\n");
            writer.write("Martin,11\n");
        }
    }

    @Benchmark()
    public void measureFileReadingIteratorForCSV(Blackhole blackhole) {
        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx = inputFactory.ctxForRefs(txnCtx, FileLineReferenceResolver::getImplementation);

        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));
        BatchIterator<Row> batchIterator = FileReadingIterator.newInstance(
            Collections.singletonList(fileUri),
            inputs,
            ctx.expressions(),
            null,
            ImmutableMap.of(
                LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            CSV);

        while (batchIterator.moveNext()) {
            blackhole.consume(batchIterator.currentElement().get(0));
        }
    }

    @TearDown
    public void cleanup() throws InterruptedException {
        tempFile.deleteOnExit();
    }
}
