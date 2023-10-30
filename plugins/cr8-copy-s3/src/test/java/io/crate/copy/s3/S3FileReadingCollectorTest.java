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

package io.crate.copy.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.crate.copy.s3.common.S3ClientHelper;
import io.crate.data.BatchIterator;
import io.crate.execution.engine.collect.files.FileReadingIterator;
import io.crate.execution.engine.collect.files.FileReadingIterator.LineCursor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3FileReadingCollectorTest extends ESTestCase {
    private static ThreadPool THREAD_POOL;

    @BeforeClass
    public static void setUpClass() throws Exception {
        THREAD_POOL = new TestThreadPool(Thread.currentThread().getName());
    }


    @AfterClass
    public static void tearDownClass() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testCollectFromS3Uri() throws Throwable {
        // this test just verifies the s3 schema detection and bucketName / prefix extraction from the uri.
        // real s3 interaction is mocked completely.
        InputStream inputStream = mock(InputStream.class);
        when(inputStream.read(any(byte[].class), Mockito.anyInt(), Mockito.anyInt())).thenReturn(-1);

        FileReadingIterator it = createBatchIterator(inputStream, "s3://fakebucket/foo");
        assertThat(it.moveNext()).isFalse();
    }

    @Test
    public void testCollectWithOneSocketTimeout() throws Throwable {
        InputStream inputStream = mock(InputStream.class);

        when(inputStream.read(any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
            .thenAnswer(new WriteBufferAnswer(new byte[]{102, 111, 111, 10}))  // first line: foo
            .thenThrow(new SocketTimeoutException())  // exception causes retry
            .thenAnswer(new WriteBufferAnswer(new byte[]{102, 111, 111, 10}))  // first line again, because of retry
            .thenAnswer(new WriteBufferAnswer(new byte[]{98, 97, 114, 10}))  // second line: bar
            .thenReturn(-1);

        FileReadingIterator it = createBatchIterator(inputStream, "s3://fakebucket/foo");
        BatchIterator<LineCursor> immutableLines = it.map(LineCursor::copy);
        List<LineCursor> lines = immutableLines.toList().get(5, TimeUnit.SECONDS);
        assertThat(lines).satisfiesExactly(
            line1 -> assertThat(line1.line()).isEqualTo("foo"),
            line1 -> assertThat(line1.line()).isEqualTo("bar")
        );
    }


    private FileReadingIterator createBatchIterator(InputStream inputStream, String ... fileUris) {
        String compression = null;
        return new FileReadingIterator(
            Arrays.asList(fileUris),
            compression,
            Map.of(
                S3FileInputFactory.NAME,
                (uri, withClauseOptions) -> new S3FileInput(new S3ClientHelper() {
                    @Override
                    protected S3Client initClient(String accessKey, String secretKey, String endpoint, String protocol) {
                        S3Client client = mock(S3Client.class);
                        var objectListing = mock(ListObjectsV2Iterable.class);
                        var s3Objects = List.of(
                            ListObjectsV2Response.builder().contents(S3Object.builder().key("foo").build()).build());

                        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(objectListing);
                        when(objectListing.iterator()).thenReturn(s3Objects.iterator());
                        when(client.getObject(GetObjectRequest.builder().bucket("fakebucket").key("foo").build()))
                            .thenReturn(new ResponseInputStream<>(GetObjectResponse.builder().build(), inputStream));
                        return client;
                    }
                }, uri, "https")),
            false,
            1,
            0,
            Settings.EMPTY,
            THREAD_POOL.scheduler());
    }

    private record WriteBufferAnswer(byte[] bytes) implements Answer<Integer> {

        @Override
        public Integer answer(InvocationOnMock invocation) {
            byte[] buffer = (byte[]) invocation.getArguments()[0];
            System.arraycopy(bytes, 0, buffer, 0, bytes.length);
            return bytes.length;
        }
    }
}
