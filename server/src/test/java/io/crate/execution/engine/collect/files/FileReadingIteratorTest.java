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

package io.crate.execution.engine.collect.files;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;
import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.JSON;
import static io.crate.testing.TestingHelpers.createReference;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.testing.BatchIteratorTester;
import io.crate.types.DataTypes;

public class FileReadingIteratorTest extends ESTestCase {

    private static final String JSON_AS_MAP_FIRST_LINE = "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}";
    private static final String JSON_AS_MAP_SECOND_LINE = "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}";
    private static final String CSV_AS_MAP_FIRST_LINE = "{\"name\":\"Arthur\",\"id\":\"4\",\"age\":\"38\"}";
    private static final String CSV_AS_MAP_SECOND_LINE = "{\"name\":\"Trillian\",\"id\":\"5\",\"age\":\"33\"}";
    private static final TransactionContext TXN_CTX = CoordinatorTxnCtx.systemTransactionContext();

    private InputFactory inputFactory;

    @Before
    public void prepare() {
        NodeContext nodeCtx = new NodeContext(new Functions(Map.of()), null);
        inputFactory = new InputFactory(nodeCtx);
    }


    @Test
    public void testIteratorContract_givenCSVInputFormat__AndNoRelevantFileExtension_thenWritesAsMap() throws Exception {
        var tempFilePath = createTempFile("tempfile", ".any-suffix");
        var tmpFile = tempFilePath.toFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
          //  writer.write("name,id,age\n");
            writer.write("Arthur,4,38\n");
            writer.write("Trillian,5,33\n");
        }
        var fileUri = tempFilePath.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), CSV
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{CSV_AS_MAP_FIRST_LINE},
            new Object[]{CSV_AS_MAP_SECOND_LINE});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }


    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndCSVExtension_thenWritesAsMap() throws Exception {
        var tempFilePath = createTempFile("tempfile", ".csv");
        var tmpFile = tempFilePath.toFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
          //  writer.write("name,id,age\n");
            writer.write("Arthur,4,38\n");
            writer.write("Trillian,5,33\n");
        }
        var fileUri = tempFilePath.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), CSV
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{CSV_AS_MAP_FIRST_LINE},
            new Object[]{CSV_AS_MAP_SECOND_LINE});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }


    private BatchIterator<Row> createBatchIterator(Collection<String> fileUris,
                                                   FileUriCollectPhase.InputFormat format) {
        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(TXN_CTX, FileLineReferenceResolver::getImplementation);

        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));
        return FileReadingIterator.newInstance(
            fileUris,
            inputs,
            ctx.expressions(),
            null,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            List.of("name", "id", "age"),
            new CopyFromParserProperties(false, false, ','), // only no-header case is considered currently
            format,
            Settings.EMPTY);
    }
}
