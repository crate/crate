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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.test.IntegTestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.SUITE, numDataNodes = 2)
@WindowsIncompatible
public class BlobIntegrationTest extends BlobHttpIntegrationTest {

    private String uploadSmallBlob() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri(digest), "a".repeat(1500));
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        return digest;
    }

    private String uploadBigBlob() throws IOException {
        String digest = "37ca53ed215ea5e0e7fb67e5e12b4ff41dd5eeb0";
        put(blobUri(digest), "abcdefghijklmnopqrstuvwxyz".repeat(1024 * 600));
        return digest;
    }

    private String uploadTinyBlob() throws IOException {
        String digest = "32d10c7b8cf96570ca04ce37f2a19d84240d3a89";
        put(blobUri(digest), "abcdefghijklmnopqrstuvwxyz");
        return digest;
    }

    @Test
    public void testUploadInvalidSha1() throws IOException {
        CloseableHttpResponse response = put("test/d937ea65641c23fadc83616309e5b0e11acc5806", "asdf");
        assertThat(response.getStatusLine().getStatusCode(), is(400));
    }

    @Test
    public void testCorsHeadersAreSet() throws Exception {
        String digest = uploadTinyBlob();
        CloseableHttpResponse response = get(blobUri(digest));
        assertThat(response.containsHeader("Access-Control-Allow-Origin")).isTrue();
    }

    @Test
    public void testNonExistingFile() throws IOException {
        CloseableHttpResponse response = get("test/d937ea65641c23fadc83616309e5b0e11acc5806");
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testErrorResponseResetsBlobHandlerStateCorrectly() throws IOException {
        String digest = uploadTinyBlob();
        CloseableHttpResponse response;

        response = get(blobUri("0000000000000000000000000000000000000000"));
        assertThat(response.getStatusLine().getStatusCode(), is(404));

        response = get(blobUri(digest));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    @Test
    public void testUploadValidFile() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri(digest), "a".repeat(1500));
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        /* Note that the content length is specified in the response in order to
        let keep alive clients know that they don't have to wait for data
        after the put and may close the connection if appropriate */
        assertThat(response.getFirstHeader("Content-Length").getValue(), is("0"));
    }

    @Test
    public void testUploadChunkedWithConflict() throws IOException {
        String digest = uploadBigBlob();
        CloseableHttpResponse conflictRes = put(blobUri(digest), "abcdefghijklmnopqrstuvwxyz".repeat(1024 * 600));
        assertThat(conflictRes.getStatusLine().getStatusCode(), is(409));
    }

    @Test
    public void testUploadToUnknownBlobTable() throws IOException {
        String digest = "c520e6109835c876fd98636efec43dd61634b7d3";
        CloseableHttpResponse response = put(blobUri("test_no_blobs", digest), "a".repeat(1500));
        assertThat(response.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testGetFiles() throws IOException {
        String digest = uploadBigBlob();
        CloseableHttpResponse res = get(blobUri(digest));
        assertThat(res.getEntity().getContentLength(), is(15974400L));
    }

    @Test
    public void testHeadRequest() throws IOException {
        String digest = uploadSmallBlob();
        CloseableHttpResponse res = head(blobUri(digest));
        assertThat(res.getFirstHeader("Content-Length").getValue(), is("1500"));
        assertThat(res.getFirstHeader("Accept-Ranges").getValue(), is("bytes"));
        assertThat(res.getFirstHeader("Expires").getValue(), is("Thu, 31 Dec 2037 23:59:59 GMT"));
        assertThat(res.getFirstHeader("Cache-Control").getValue(), is("max-age=315360000"));
    }

    @Test
    public void testNodeWhichHasTheBlobDoesntRedirect() throws IOException {
        // One of the head requests must be redirected:
        String digest = uploadSmallBlob();

        int numberOfRedirects1 = getNumberOfRedirects(blobUri(digest), dataNode1);
        assertThat(numberOfRedirects1, greaterThanOrEqualTo(0));

        int numberOfRedirects2 = getNumberOfRedirects(blobUri(digest), dataNode2);
        assertThat(numberOfRedirects2, greaterThanOrEqualTo(0));

        assertThat("The node where the blob resides should not issue a redirect",
            numberOfRedirects1, is(not(numberOfRedirects2)));
    }

    @Test
    public void testDeleteFile() throws IOException {
        String digest = uploadSmallBlob();
        String uri = blobUri(digest);
        CloseableHttpResponse res = delete(uri);
        assertThat(res.getStatusLine().getStatusCode(), is(204));

        res = get(uri);
        assertThat(res.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testByteRange() throws IOException {
        String digest = uploadTinyBlob();
        Header[] headers = {
            new BasicHeader("Range", "bytes=8-")
        };
        CloseableHttpResponse res = get(blobUri(digest), headers);
        assertThat(res.getFirstHeader("Content-Length").getValue(), is("18"));
        assertThat(res.getFirstHeader("Content-Range").getValue(), is("bytes 8-25/26"));
        assertThat(res.getFirstHeader("Accept-Ranges").getValue(), is("bytes"));
        assertThat(res.getFirstHeader("Expires").getValue(), is("Thu, 31 Dec 2037 23:59:59 GMT"));
        assertThat(res.getFirstHeader("Cache-Control").getValue(), is("max-age=315360000"));
        assertThat(EntityUtils.toString(res.getEntity()), is("ijklmnopqrstuvwxyz"));

        res = get(blobUri(digest), new Header[]{
            new BasicHeader("Range", "bytes=0-1")
        });
        assertThat(EntityUtils.toString(res.getEntity()), is("ab"));

        res = get(blobUri(digest), new Header[]{
            new BasicHeader("Range", "bytes=25-")
        });
        assertThat(EntityUtils.toString(res.getEntity()), is("z"));
    }

    @Test
    public void testInvalidByteRange() throws IOException {
        String digest = uploadTinyBlob();
        Header[] headers = {
            new BasicHeader("Range", "bytes=40-58")
        };
        CloseableHttpResponse res = get(blobUri(digest), headers);
        assertThat(res.getStatusLine().getStatusCode(), is(416));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Requested Range Not Satisfiable"));
        assertThat(res.getFirstHeader("Content-Length").getValue(), is("0"));
    }

    @Test
    public void testParallelAccess() throws Throwable {
        String digest = uploadBigBlob();
        String expectedContent = "abcdefghijklmnopqrstuvwxyz".repeat(1024 * 600);
        Header[][] headers = new Header[40][];
        String[] uris = new String[40];
        String[] expected = new String[40];
        for (int i = 0; i < 40; i++) {
            headers[i] = new Header[]{};
            uris[i] = blobUri(digest);
            expected[i] = expectedContent;
        }
        assertThat(mget(uris, headers, expected)).isTrue();
    }

    @Test
    public void testParallelAccessWithRange() throws Throwable {
        String digest = uploadBigBlob();
        String expectedContent = "abcdefghijklmnopqrstuvwxyz".repeat(1024 * 600);
        Header[][] headers = new Header[][]{
            {new BasicHeader("Range", "bytes=0-")},
            {new BasicHeader("Range", "bytes=10-100")},
            {new BasicHeader("Range", "bytes=20-30")},
            {new BasicHeader("Range", "bytes=40-50")},
            {new BasicHeader("Range", "bytes=40-80")},
            {new BasicHeader("Range", "bytes=10-80")},
            {new BasicHeader("Range", "bytes=5-30")},
            {new BasicHeader("Range", "bytes=15-3000")},
            {new BasicHeader("Range", "bytes=2000-10800")},
            {new BasicHeader("Range", "bytes=1500-20000")},
        };
        String[] expected = new String[]{
            expectedContent,
            expectedContent.substring(10, 101),
            expectedContent.substring(20, 31),
            expectedContent.substring(40, 51),
            expectedContent.substring(40, 81),
            expectedContent.substring(10, 81),
            expectedContent.substring(5, 31),
            expectedContent.substring(15, 3001),
            expectedContent.substring(2000, 10801),
            expectedContent.substring(1500, 20001),
        };
        String[] uris = new String[10];
        for (int i = 0; i < 10; i++) {
            uris[i] = blobUri(digest);
        }
        assertThat(mget(uris, headers, expected)).isTrue();
    }

    @Test
    public void testHeadRequestConnectionIsNotClosed() throws Exception {
        Socket socket = new Socket(randomNode.getAddress(), randomNode.getPort());
        socket.setKeepAlive(true);
        socket.setSoTimeout(3000);

        OutputStream outputStream = socket.getOutputStream();
        outputStream.write("HEAD /_blobs/invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n"
            .getBytes(StandardCharsets.UTF_8));
        outputStream.write("Host: localhost\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        int linesRead = 0;
        while (linesRead < 3) {
            String line = reader.readLine();
            System.out.println(line);
            linesRead++;
        }

        assertSocketIsConnected(socket);
        outputStream.write("HEAD /_blobs/invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n"
            .getBytes(StandardCharsets.UTF_8));
        outputStream.write("Host: localhost\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        int read = reader.read();
        assertThat(read, greaterThan(-1));
        assertSocketIsConnected(socket);
    }

    @Test
    public void testResponseContainsCloseHeaderOnHttp10() throws Exception {
        Socket socket = new Socket(randomNode.getAddress(), randomNode.getPort());
        socket.setKeepAlive(false);
        socket.setSoTimeout(3000);

        OutputStream outputStream = socket.getOutputStream();
        outputStream.write("HEAD /_blobs/invalid/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.0\r\n"
            .getBytes(StandardCharsets.UTF_8));
        outputStream.write("Host: localhost\r\n\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        String line;
        List<String> lines = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        assertThat(lines, hasItem("connection: close"));
    }

    private void assertSocketIsConnected(Socket socket) {
        assertThat(socket.isConnected()).isTrue();
        assertThat(socket.isClosed()).isFalse();
        assertThat(socket.isInputShutdown()).isFalse();
        assertThat(socket.isOutputShutdown()).isFalse();
    }

    @Test
    public void testEmptyFile() throws IOException {
        CloseableHttpResponse res = put(blobUri("da39a3ee5e6b4b0d3255bfef95601890afd80709"), "");
        assertThat(res.getStatusLine().getStatusCode(), is(201));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Created"));

        res = put(blobUri("da39a3ee5e6b4b0d3255bfef95601890afd80709"), "");
        assertThat(res.getStatusLine().getStatusCode(), is(409));
        assertThat(res.getStatusLine().getReasonPhrase(), is("Conflict"));
    }

    @Test
    public void testGetInvalidDigest() throws Exception {
        CloseableHttpResponse resp = get(blobUri("invlaid"));
        assertThat(resp.getStatusLine().getStatusCode(), is(404));
    }

    @Test
    public void testBlobShardIncrementalStatsUpdate() throws IOException {
        String digest = uploadSmallBlob();
        BlobShard blobShard = getBlobShard(digest);

        if (blobShard == null) {
            fail("Unable to find blob shard");
        }

        assertThat(blobShard.getBlobsCount(), is(1L));
        assertThat(blobShard.getTotalSize(), greaterThan(0L));

        String uri = blobUri(digest);
        delete(uri);
        assertThat(blobShard.getBlobsCount(), is(0L));
        assertThat(blobShard.getTotalSize(), is(0L));

        // attempting to delete the same digest multiple times doesn't modify the stats
        delete(uri);
        assertThat(blobShard.getBlobsCount(), is(0L));
        assertThat(blobShard.getTotalSize(), is(0L));
    }

    @Test
    public void testBlobShardStatsWhenTheSameBlobIsConcurrentlyUploaded() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        CountDownLatch latch = new CountDownLatch(2);
        List<CompletableFuture<String>> blobUploads = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            blobUploads.add(CompletableFuture.supplyAsync(() -> {
                try {
                    latch.countDown();
                    latch.await(10, TimeUnit.SECONDS);
                    return uploadBigBlob();
                } catch (Exception e) {
                    fail("Expecting successful upload but got: " + e.getMessage());
                }
                return null;
            }, executorService));
        }

        try {
            String digest = null;
            for (CompletableFuture<String> blobUpload : blobUploads) {
                digest = blobUpload.join();
            }

            BlobShard blobShard = getBlobShard(digest);
            if (blobShard == null) {
                fail("Unable to find blob shard");
            }

            assertThat(blobShard.getBlobsCount(), is(1L));
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Nullable
    private BlobShard getBlobShard(String digest) {
        Iterable<BlobIndicesService> services = cluster().getInstances(BlobIndicesService.class);
        Iterator<BlobIndicesService> it = services.iterator();
        BlobShard blobShard = null;
        while (it.hasNext()) {
            BlobIndicesService nextService = it.next();
            try {
                blobShard = nextService.localBlobShard(".blob_test", digest);
            } catch (ShardNotFoundException | IndexNotFoundException e) {
                continue;
            }
            if (blobShard != null) {
                break;
            }
        }
        return blobShard;
    }

}
