/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect.files;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.external.S3ClientHelper;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class FileReadingCollectorTest extends CrateUnitTest {

    private static File tmpFile;
    private static File tmpFileGz;
    private static File tmpFileEmptyLine;
    private FileCollectInputSymbolVisitor inputSymbolVisitor;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path copy_from = Files.createTempDirectory("copy_from");
        Path copy_from_gz = Files.createTempDirectory("copy_from_gz");
        Path copy_from_empty = Files.createTempDirectory("copy_from_empty");
        tmpFileGz = File.createTempFile("fileReadingCollector", ".json.gz", copy_from_gz.toFile());
        tmpFile = File.createTempFile("fileReadingCollector", ".json", copy_from.toFile());
        tmpFileEmptyLine = File.createTempFile("emptyLine", ".json", copy_from_empty.toFile());
        try (BufferedWriter writer =
                     new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tmpFileGz))))) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
        try (FileWriter writer = new FileWriter(tmpFile)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
        try (FileWriter writer = new FileWriter(tmpFileEmptyLine)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }
    }

    @Before
    public void prepare() throws Exception {
        Functions functions = new Functions(
                ImmutableMap.<FunctionIdent, FunctionImplementation>of(),
                ImmutableMap.<String, DynamicFunctionResolver>of(),
                Collections.<String, TableFunctionImplementation>emptyMap()
        );
        inputSymbolVisitor =
                new FileCollectInputSymbolVisitor(functions, FileLineReferenceResolver.INSTANCE);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        tmpFile.delete();
        tmpFileGz.delete();
        tmpFileEmptyLine.delete();
    }

    @Test
    public void testUmlautsAndWhitespacesWithExplicitURIThrowsAre() throws Throwable {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal character in path at index 12: file:///this will fäil.json");
        getObjects("file:///this will fäil.json");
    }

    @Test
    public void testCollectFromS3Uri() throws Throwable {
        // this test just verifies the s3 schema detection and bucketName / prefix extraction from the uri.
        // real s3 interaction is mocked completely.
        CollectingRowReceiver projector = getObjects("s3://fakebucket/foo");
        projector.result();
    }

    @Test
    public void testNoErrorIfNoSuchFile() throws Throwable {
        // no error, -> don't want to fail just because one node doesn't have a file
        getObjects("/some/path/that/shouldnt/exist/foo.json");
        getObjects("/some/path/that/shouldnt/exist/*");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testRelativeImport() throws Throwable {
        CollectingRowReceiver projector = getObjects("xy");
        assertCorrectResult(projector.result());
    }

    @Test
    public void testCollectFromUriWithGlob() throws Throwable {
        CollectingRowReceiver projector = getObjects(resolveURI(tmpFile.getParentFile(), "file*.json"));
        assertCorrectResult(projector.result());
    }

    @Test
    public void testCollectFromDirectory() throws Throwable {
        CollectingRowReceiver projector = getObjects(resolveURI(tmpFile.getParentFile(), "*"));
        assertCorrectResult(projector.result());
    }

    @Test
    public void testDoCollectRaw() throws Throwable {
        CollectingRowReceiver projector = getObjects(Paths.get(tmpFile.toURI()).toUri().toString());
        assertCorrectResult(projector.result());
    }

    @Test
    public void testDoCollectRawFromCompressed() throws Throwable {
        CollectingRowReceiver projector = getObjects(Collections.singletonList(Paths.get(tmpFileGz.toURI()).toUri().toString()), "gzip");
        assertCorrectResult(projector.result());
    }

    @Test
    public void testCollectWithEmptyLine() throws Throwable {
        CollectingRowReceiver projector = getObjects(Paths.get(tmpFileEmptyLine.toURI()).toUri().toString());
        assertCorrectResult(projector.result());
    }

    @Test
    public void testCollectWithOneSocketTimeout() throws Throwable {
        S3ObjectInputStream inputStream = mock(S3ObjectInputStream.class);

        when(inputStream.read(new byte[anyInt()], anyInt(), anyByte()))
                .thenAnswer(new WriteBufferAnswer(new byte[] { 102, 111, 111, 10}))  // first line: foo
                .thenThrow(new SocketTimeoutException())  // exception causes retry
                .thenAnswer(new WriteBufferAnswer(new byte[] { 102, 111, 111, 10}))  // first line again, because of retry
                .thenAnswer(new WriteBufferAnswer(new byte[] { 98, 97, 114, 10 }))  // second line: bar
                .thenReturn(-1);


        CollectingRowReceiver projector = getObjects(Collections.singletonList("s3://fakebucket/foo"), null, inputStream);
        Bucket rows = projector.result();
        assertThat(rows.size(), is(2));
        assertThat(TestingHelpers.printedTable(rows), is("foo\nbar\n"));
    }

    @Test
    public void unsupportedURITest() throws Throwable {
        expectedException.expectCause(isA(MalformedURLException.class));
        expectedException.expectMessage("unknown protocol: invalid");
        getObjects("invalid://crate.io/docs/en/latest/sql/reference/copy_from.html").result();
    }

    @Test
    public void testMultipleUriSupport() throws Throwable {
        List<String> fileUris = new ArrayList<>();
        fileUris.add(tmpFile.getCanonicalPath());
        fileUris.add(tmpFileEmptyLine.getCanonicalPath());
        CollectingRowReceiver projector = getObjects(fileUris, null);
        Iterator<Row> it = projector.result().iterator();
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.next(), isRow("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.next(), isRow("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
    }

    @Test
    public void testRowReceiverDontWantMoreStopsCollectingMultipleUris() throws Throwable {
        DontWantMoreRowReceiver rowReceiver = new DontWantMoreRowReceiver(1);
        List<String> fileUris = new ArrayList<>();
        fileUris.add(tmpFile.getCanonicalPath());
        fileUris.add(tmpFileEmptyLine.getCanonicalPath());
        getObjects(fileUris, null, null, rowReceiver);
        Iterator<Row> it = rowReceiver.result().iterator();
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.hasNext(), is(false));
    }

    private void assertCorrectResult(Bucket rows) throws Throwable {
        Iterator<Row> it = rows.iterator();
        assertThat(it.next(), isRow("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(it.next(), isRow("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
    }

    private CollectingRowReceiver getObjects(String fileUri) throws Throwable {
        return getObjects(Collections.singletonList(fileUri), null);
    }

    private CollectingRowReceiver getObjects(Collection<String> fileUris, String compression) throws Throwable {
        S3ObjectInputStream inputStream = mock(S3ObjectInputStream.class);
        when(inputStream.read(new byte[anyInt()], anyInt(), anyByte())).thenReturn(-1);
        return getObjects(fileUris, compression, inputStream);
    }

    private CollectingRowReceiver getObjects(Collection<String> fileUris, String compression, S3ObjectInputStream s3InputStream) throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        getObjects(fileUris, compression, s3InputStream, rowReceiver);
        return rowReceiver;
    }

    private void getObjects(Collection<String> fileUris, String compression, final S3ObjectInputStream s3InputStream, RowReceiver rowReceiver) throws Throwable {
        FileCollectInputSymbolVisitor.Context context =
                inputSymbolVisitor.extractImplementations(createReference("_raw", DataTypes.STRING));
        FileReadingCollector collector = new FileReadingCollector(
                fileUris,
                context.topLevelInputs(),
                context.expressions(),
                rowReceiver,
                FileReadingCollector.FileFormat.JSON,
                compression,
                ImmutableMap.of(
                        LocalFsFileInputFactory.NAME, new LocalFsFileInputFactory(),
                        S3FileInputFactory.NAME, new FileInputFactory() {
                    @Override
                    public FileInput create() throws IOException {
                        return new S3FileInput(new S3ClientHelper() {
                            @Override
                            protected AmazonS3 initClient(String accessKey, String secretKey) throws IOException {
                                AmazonS3 client = mock(AmazonS3Client.class);
                                ObjectListing objectListing = mock(ObjectListing.class);
                                S3ObjectSummary summary = mock(S3ObjectSummary.class);
                                S3Object s3Object = mock(S3Object.class);


                                when(client.listObjects(anyString(), anyString())).thenReturn(objectListing);
                                when(objectListing.getObjectSummaries()).thenReturn(Arrays.asList(summary));
                                when(summary.getKey()).thenReturn("foo");
                                when(client.getObject("fakebucket", "foo")).thenReturn(s3Object);
                                when(s3Object.getObjectContent()).thenReturn(s3InputStream);
                                when(client.listNextBatchOfObjects(any(ObjectListing.class))).thenReturn(objectListing);
                                when(objectListing.isTruncated()).thenReturn(false);
                                return client;
                            }
                        });
                    }
                }),
                false,
                1,
                0
        );
        rowReceiver.prepare();
        collector.doCollect();
    }

    /**
     * Resolves file path to URI<br/>
     * Note: Per URI specification, the only allowed format is file:///foo/bar.json (or for windows file:///C:/foo/bar.json)
     * @param path the path (directory) that contains the file
     * @param file the file name
     * @return uri
     */
    public static String resolveURI(File path, String file){
        String uri = null != file ?
                Joiner.on("").join(path.toURI().toString(), file) : path.toURI().toString();
        // matching against regex "file:\/[^\/].*"
        if(uri.matches("file:\\/[^\\/].*")){
            uri = uri.replace("file:/", "file:///");
        }
        return uri;
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

    static class DontWantMoreRowReceiver extends CollectingRowReceiver {
        private int stopAfter;

        public DontWantMoreRowReceiver(int stopAfter) {
            this.stopAfter = stopAfter;
        }

        @Override
        public boolean setNextRow(Row row) {
            boolean res = super.setNextRow(row);
            if (rows.size() >= stopAfter) {
                return false;
            }
            return res;
        }
    }
}
