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
import io.crate.external.S3ClientHelper;
import io.crate.metadata.DynamicFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.reference.file.FileLineReferenceResolver;
import io.crate.types.DataTypes;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class FileReadingCollectorTest extends AbstractRandomizedTest {

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
                ImmutableMap.<String, DynamicFunctionResolver>of()
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
    public void testCollectFromS3Uri() throws Throwable {
        // this test just verifies the s3 schema detection and bucketName / prefix extraction from the uri.
        // real s3 interaction is mocked completely.
        CollectingProjector projector = getObjects("s3://fakebucket/foo");
        projector.result().get();
    }

    @Test
    public void testNoErrorIfNoSuchFile() throws Throwable {
        // no error, -> don't want to fail just because one node doesn't have a file
        getObjects("/some/path/that/shouldnt/exist/foo.json");
        getObjects("/some/path/that/shouldnt/exist/*");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testRelativeImport() throws Throwable {
        CollectingProjector projector = getObjects("xy");
        assertCorrectResult(projector.result().get());
    }

    @Test
    public void testCollectFromUriWithGlob() throws Throwable {
        CollectingProjector projector = getObjects(resolveURI(tmpFile.getParentFile(), "file*.json"));
        assertCorrectResult(projector.result().get());
    }

    @Test
    public void testCollectFromDirectory() throws Throwable {
        CollectingProjector projector = getObjects(resolveURI(tmpFile.getParentFile(), "*"));
        assertCorrectResult(projector.result().get());
    }

    @Test
    public void testDoCollectRaw() throws Throwable {
        CollectingProjector projector = getObjects(Paths.get(tmpFile.toURI()).toUri().toString());
        assertCorrectResult(projector.result().get());
    }

    @Test
    public void testDoCollectRawFromCompressed() throws Throwable {
        CollectingProjector projector = getObjects(Paths.get(tmpFileGz.toURI()).toUri().toString(), "gzip");
        assertCorrectResult(projector.result().get());
    }

    private void assertCorrectResult(Object[][] rows) throws Throwable {
        assertThat(((BytesRef) rows[0][0]).utf8ToString(), is(
                "{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}"));
        assertThat(((BytesRef) rows[1][0]).utf8ToString(), is(
                "{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}"));
    }

    @Test
    public void testCollectWithEmptyLine() throws Throwable {
        CollectingProjector projector = getObjects(Paths.get(tmpFileEmptyLine.toURI()).toUri().toString());
        assertCorrectResult(projector.result().get());
    }

    private CollectingProjector getObjects(String fileUri) throws Throwable {
        return getObjects(fileUri, null);
    }

    private CollectingProjector getObjects(String fileUri, String compression) throws Throwable {
        CollectingProjector projector = new CollectingProjector();
        FileCollectInputSymbolVisitor.Context context =
                inputSymbolVisitor.process(createReference("_raw", DataTypes.STRING));
        FileReadingCollector collector = new FileReadingCollector(
                fileUri,
                context.topLevelInputs(),
                context.expressions(),
                projector,
                FileReadingCollector.FileFormat.JSON,
                compression,
                ImmutableMap.<String, FileInputFactory>of("s3", new FileInputFactory() {
                    @Override
                    public FileInput create() throws IOException {
                        return new S3FileInput(new S3ClientHelper() {
                            @Override
                            protected AmazonS3 initClient(String accessKey, String secretKey) throws IOException {
                                AmazonS3 client = mock(AmazonS3Client.class);
                                ObjectListing objectListing = mock(ObjectListing.class);
                                S3ObjectSummary summary = mock(S3ObjectSummary.class);
                                S3Object s3Object = mock(S3Object.class);

                                S3ObjectInputStream inputStream = mock(S3ObjectInputStream.class);

                                when(client.listObjects(anyString(), anyString())).thenReturn(objectListing);
                                when(objectListing.getObjectSummaries()).thenReturn(Arrays.asList(summary));
                                when(summary.getKey()).thenReturn("foo");
                                when(client.getObject("fakebucket", "foo")).thenReturn(s3Object);
                                when(s3Object.getObjectContent()).thenReturn(inputStream);
                                when(inputStream.read(new byte[anyInt()], anyInt(), anyByte())).thenReturn(-1);
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
        projector.startProjection();
        collector.doCollect(null);
        return projector;
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
}
