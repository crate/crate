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

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.crate.analyze.CopyFromParserProperties;
import io.crate.data.BatchIterator;
import io.crate.data.Bucket;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.file.FileLineReferenceResolver;
import io.crate.expression.reference.file.SourceLineExpression;
import io.crate.expression.reference.file.SourceUriFailureExpression;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.testing.TestingRowConsumer;
import io.crate.types.DataTypes;

public class FileReadingCollectorTest extends ESTestCase {

    private static File tmpFile;
    private static File tmpFileGz;
    private static File tmpFileEmptyLine;
    private InputFactory inputFactory;
    private Input<String> sourceUriFailureInput;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path copy_from = Files.createTempDirectory("copy_from");
        Path copy_from_gz = Files.createTempDirectory("copy_from_gz");
        Path copy_from_empty = Files.createTempDirectory("copy_from_empty");
        tmpFileGz = File.createTempFile("fileReadingCollector", ".json.gz", copy_from_gz.toFile());
        tmpFile = File.createTempFile("fileReadingCollector", ".json", copy_from.toFile());
        tmpFileEmptyLine = File.createTempFile("emptyLine", ".json", copy_from_empty.toFile());
        try (BufferedWriter writer =
                 new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tmpFileGz)),
                     StandardCharsets.UTF_8))) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(tmpFileEmptyLine), StandardCharsets.UTF_8)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
    }

    @Before
    public void prepare() throws Exception {
        NodeContext nodeCtx = new NodeContext(new Functions(Map.of()), null);
        inputFactory = new InputFactory(nodeCtx);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        assertThat(tmpFile.delete(), is(true));
        assertThat(tmpFileGz.delete(), is(true));
        assertThat(tmpFileEmptyLine.delete(), is(true));
    }

    @Test
    public void testUmlautsAndWhitespacesWithExplicitURIThrowsAre() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal character in path at index 12: file:///this will fäil.json");
        getObjects("file:///this will fäil.json");
    }

    @Test
    public void testNoErrorIfNoSuchFile() throws Throwable {
        // no error, -> don't want to fail just because one node doesn't have a file
        getObjects("file:///some/path/that/shouldnt/exist/foo.json");
        getObjects("file:///some/path/that/shouldnt/exist/*");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRelativeImport() throws Throwable {
        TestingRowConsumer projector = getObjects("xy");
        assertCorrectResult(projector.getBucket());
    }

    @Test
    public void testCollectFromUriWithGlob() throws Throwable {
        TestingRowConsumer projector = getObjects(
            Paths.get(tmpFile.getParentFile().toURI()).toUri().toString() + "file*.json");
        assertCorrectResult(projector.getBucket());
    }

    @Test
    public void testCollectFromDirectory() throws Throwable {
        TestingRowConsumer projector = getObjects(
            Paths.get(tmpFile.getParentFile().toURI()).toUri().toString() + "*");
        assertCorrectResult(projector.getBucket());
    }

    @Test
    public void testDoCollectRaw() throws Throwable {
        TestingRowConsumer consumer = getObjects(Paths.get(tmpFile.toURI()).toUri().toString());
        assertCorrectResult(consumer.getBucket());
    }

    @Test
    public void testDoCollectRawFromCompressed() throws Throwable {
        TestingRowConsumer consumer = getObjects(Collections.singletonList(Paths.get(tmpFileGz.toURI()).toUri().toString()), "gzip");
        assertCorrectResult(consumer.getBucket());
    }

    @Test
    public void testCollectWithEmptyLine() throws Throwable {
        TestingRowConsumer consumer = getObjects(Paths.get(tmpFileEmptyLine.toURI()).toUri().toString());
        assertCorrectResult(consumer.getBucket());
    }

    @Test
    public void unsupportedURITest() throws Throwable {
        getObjects("invalid://crate.io/docs/en/latest/sql/reference/copy_from.html", true).getBucket();
        assertThat(sourceUriFailureInput.value(), is("unknown protocol: invalid"));
    }

    @Test
    public void testMultipleUriSupport() throws Throwable {
        List<String> fileUris = new ArrayList<>();
        fileUris.add(Paths.get(tmpFile.toURI()).toUri().toString());
        fileUris.add(Paths.get(tmpFileEmptyLine.toURI()).toUri().toString());
        TestingRowConsumer consumer = getObjects(fileUris, null);
        Iterator<Row> it = consumer.getBucket().iterator();
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.next(), isRow("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.next(), isRow("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
    }

    private void assertCorrectResult(Bucket rows) throws Throwable {
        Iterator<Row> it = rows.iterator();
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.next(), isRow("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
    }

    private TestingRowConsumer getObjects(String fileUri) throws Throwable {
        return getObjects(fileUri, false);
    }

    private TestingRowConsumer getObjects(String fileUri, boolean collectSourceUriFailure) throws Throwable {
        return getObjects(Collections.singletonList(fileUri), null, collectSourceUriFailure);
    }

    private TestingRowConsumer getObjects(Collection<String> fileUris,
                                          String compression) throws Throwable {
        return getObjects(fileUris, compression, false);
    }

    private TestingRowConsumer getObjects(Collection<String> fileUris,
                                          String compression,
                                          boolean collectSourceUriFailure) throws Throwable {
        TestingRowConsumer consumer = new TestingRowConsumer();
        getObjects(fileUris, compression, consumer, collectSourceUriFailure);
        return consumer;
    }

    private void getObjects(Collection<String> fileUris,
                            String compression,
                            RowConsumer consumer,
                            boolean collectSourceUriFailure) throws Throwable {
        BatchIterator<Row> iterator = createBatchIterator(fileUris, compression, collectSourceUriFailure);
        consumer.accept(iterator, null);
    }

    private BatchIterator<Row> createBatchIterator(Collection<String> fileUris,
                                                   String compression,
                                                   boolean collectSourceUriFailure) {
        InputFactory.Context<LineCollectorExpression<?>> ctx =
            inputFactory.ctxForRefs(txnCtx, FileLineReferenceResolver::getImplementation);
        List<Input<?>> inputs = new ArrayList<>(2);
        Reference raw = createReference(SourceLineExpression.COLUMN_NAME, DataTypes.STRING);
        inputs.add(ctx.add(raw));
        if (collectSourceUriFailure) {
            Reference sourceUriFailure = createReference(SourceUriFailureExpression.COLUMN_NAME, DataTypes.STRING);
            //noinspection unchecked
            sourceUriFailureInput = (Input<String>) ctx.add(sourceUriFailure);
            inputs.add(sourceUriFailureInput);
        }
        return FileReadingIterator.newInstance(
            fileUris.stream().map(FileReadingIterator::toURI).toList(),
            inputs,
            ctx.expressions(),
            compression,
            Map.of(LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory()),
            false,
            1,
            0,
            List.of("a", "b"),
            CopyFromParserProperties.DEFAULT,
            FileUriCollectPhase.InputFormat.JSON,
            Settings.EMPTY);
    }

    private static class WriteBufferAnswer implements Answer<Integer> {

        private byte[] bytes;

        public WriteBufferAnswer(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public Integer answer(InvocationOnMock invocation) throws Throwable {
            byte[] buffer = (byte[]) invocation.getArguments()[0];
            System.arraycopy(bytes, 0, buffer, 0, bytes.length);
            return bytes.length;
        }
    }
}
