/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect.files;

import com.google.common.collect.ImmutableMap;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.CSV;
import static io.crate.execution.dsl.phases.FileUriCollectPhase.InputFormat.JSON;
import static io.crate.testing.TestingHelpers.createReference;

public class FileReadingIteratorTest extends CrateUnitTest {

    private InputFactory inputFactory;
    private Path tempFilePath;
    private String fileUri;
    private File tmpFile;
    private byte[] JSON_AS_MAP_FIRST_LINE = "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}".getBytes(StandardCharsets.UTF_8);
    private byte[] JSON_AS_MAP_SECOND_LINE = "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}".getBytes(StandardCharsets.UTF_8);
    private byte[] CSV_AS_MAP_FIRST_LINE = "{\"name\":\"Arthur\",\"id\":\"4\",\"age\":\"38\"}".getBytes(StandardCharsets.UTF_8);
    private byte[] CSV_AS_MAP_SECOND_LINE = "{\"name\":\"Trillian\",\"id\":\"5\",\"age\":\"33\"}".getBytes(StandardCharsets.UTF_8);

    @Before
    public void prepare() {
        Functions functions = new Functions(
            ImmutableMap.<FunctionIdent, FunctionImplementation>of(),
            ImmutableMap.<String, FunctionResolver>of()
        );
        inputFactory = new InputFactory(functions);
    }

    @Test
    public void testIteratorContract_givenJSONInputFormat_AndNoRelevantFileExtension_thenWritesAsMap() throws Exception {
        tempFilePath = createTempFile("tempfile", ".any-suffix");
        tmpFile = tempFilePath.toFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
        fileUri = tempFilePath.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), null, JSON
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{new BytesRef(JSON_AS_MAP_FIRST_LINE)},
            new Object[]{new BytesRef(JSON_AS_MAP_SECOND_LINE)});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenCSVInputFormat__AndNoRelevantFileExtension_thenWritesAsMap() throws Exception {
        tempFilePath = createTempFile("tempfile", ".any-suffix");
        tmpFile = tempFilePath.toFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("name,id,age\n");
            writer.write("Arthur,4,38\n");
            writer.write("Trillian,5,33\n");
        }
        fileUri = tempFilePath.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), null, CSV
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{new BytesRef(CSV_AS_MAP_FIRST_LINE)},
            new Object[]{new BytesRef(CSV_AS_MAP_SECOND_LINE)});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndJSONExtension_thenWritesAsMap() throws Exception {
        tempFilePath = createTempFile("tempfile", ".json");
        tmpFile = tempFilePath.toFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
        fileUri = tempFilePath.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), null, JSON
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{new BytesRef(JSON_AS_MAP_FIRST_LINE)},
            new Object[]{new BytesRef(JSON_AS_MAP_SECOND_LINE)});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void testIteratorContract_givenDefaultJsonInputFormat_AndCSVExtension_thenWritesAsMap() throws Exception {
        tempFilePath = createTempFile("tempfile", ".csv");
        tmpFile = tempFilePath.toFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("name,id,age\n");
            writer.write("Arthur,4,38\n");
            writer.write("Trillian,5,33\n");
        }
        fileUri = tempFilePath.toUri().toString();

        Supplier<BatchIterator<Row>> batchIteratorSupplier = () -> createBatchIterator(
            Collections.singletonList(fileUri), null, JSON
        );

        List<Object[]> expectedResult = Arrays.asList(
            new Object[]{new BytesRef(CSV_AS_MAP_FIRST_LINE)},
            new Object[]{new BytesRef(CSV_AS_MAP_SECOND_LINE)});
        BatchIteratorTester tester = new BatchIteratorTester(batchIteratorSupplier);
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    private BatchIterator<Row> createBatchIterator(Collection<String> fileUris, String compression, FileUriCollectPhase.InputFormat format) {
        Reference raw = createReference("_raw", DataTypes.STRING);
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(FileLineReferenceResolver::getImplementation);

        List<Input<?>> inputs = Collections.singletonList(ctx.add(raw));
        return FileReadingIterator.newInstance(
            fileUris,
            inputs,
            ctx.expressions(),
            compression,
            ImmutableMap.of(
                LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            format);
    }
}
