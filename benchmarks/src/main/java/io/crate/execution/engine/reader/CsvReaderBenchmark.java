/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.reader;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import io.crate.execution.engine.collect.files.LineCollectorExpression;
import io.crate.execution.engine.collect.files.LocalFsFileInputFactory;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class CsvReaderBenchmark {

    private String fileUri;
    private InputFactory inputFactory;
    private TransactionContext txnCtx = TransactionContext.of(
        new SessionSettings("dummyUser",
                            SearchPath.createSearchPathFrom("dummySchema")));
    File tempFile;

    public static Reference createReference(String columnName, DataType<?> dataType) {
        return new SimpleReference(
            new ReferenceIdent(
                new RelationName(Schemas.DOC_SCHEMA_NAME, "dummyTable"),
                new ColumnIdent(columnName)
            ),
            RowGranularity.DOC,
            dataType,
            0,
            null
        );
    }

    @Setup
    public void create_temp_file_and_uri() throws IOException {
        NodeContext nodeCtx = new NodeContext(new Functions(Map.of()), null);
        inputFactory = new InputFactory(nodeCtx);
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
            List.of(FileReadingIterator.toURI(fileUri)),
            inputs,
            ctx.expressions(),
            null,
            Map.of(
                LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            List.of("id", "name"),
            CopyFromParserProperties.DEFAULT,
            CSV,
            Settings.EMPTY);

        while (batchIterator.moveNext()) {
            blackhole.consume(batchIterator.currentElement().get(0));
        }
    }

    @TearDown
    public void cleanup() throws InterruptedException {
        tempFile.delete();
    }
}
