/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.protocols.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.jspecify.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import io.crate.blob.BlobContainer;
import io.crate.blob.BlobService;
import io.crate.blob.RemoteDigestBlob;
import io.crate.blob.exceptions.MissingHTTPEndpointException;
import io.crate.blob.v2.BlobShard;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.session.Session;
import io.crate.session.Sessions;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class HttpBlobHandlerTest extends CrateDummyClusterServiceUnitTest {

    private static final String VALID_DIGEST = "a".repeat(40);
    private static final String BLOB_URI = "/_blobs/mytable/" + VALID_DIGEST;

    // An instance for cases where it returns some data and doesn't throw.
    private final BlobService blobService = mock(BlobService.class);
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setup() throws Throwable {
        when(blobService.getRedirectAddress(anyString(), anyString())).thenReturn(null);
        BlobShard blobShard = mock(BlobShard.class);
        Path tmpDirPath = createTempDir("dummy-dir");
        Files.write(Files.createTempFile(tmpDirPath, "dummyFile", ""), new byte[]{1, 2, 3, 4, 5});
        BlobContainer container = new BlobContainer(tmpDirPath);
        when(blobService.localBlobShard(anyString(), anyString())).thenReturn(blobShard);
        when(blobShard.blobContainer()).thenReturn(container);
        embeddedChannel = createEmbeddedChannel(blobService);
    }

    @Test
    public void test_non_matching_blob_uri_does_not_leak_request() {
        FullHttpRequest req = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/_blobs/bad-uri-no-digest");
        assertThat(req.refCnt()).isEqualTo(1);
        embeddedChannel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_missing_http_endpoint_does_not_leak_request() throws Throwable {
        BlobService blobService = mock(BlobService.class);
        when(blobService.getRedirectAddress(anyString(), anyString()))
            .thenThrow(new MissingHTTPEndpointException("dummy"));

        EmbeddedChannel channel = createEmbeddedChannel(blobService);
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        channel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_redirect_does_not_leak_request() throws Throwable {
        BlobService blobService = mock(BlobService.class);
        when(blobService.getRedirectAddress(anyString(), anyString()))
            .thenReturn("dummy");

        EmbeddedChannel channel = createEmbeddedChannel(blobService);
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        channel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_get_partial_content_invalid_range_does_not_leak_request() throws Throwable {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        req.headers().set(io.netty.handler.codec.http.HttpHeaderNames.RANGE, "invalid");
        embeddedChannel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_get_partial_content_not_satisfiable_range_does_not_leak_request() throws Throwable {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        req.headers().set(io.netty.handler.codec.http.HttpHeaderNames.RANGE, "bytes=100-200");
        embeddedChannel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_get_partial_content_valid_range_does_not_leak_request() throws Throwable {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        req.headers().set(io.netty.handler.codec.http.HttpHeaderNames.RANGE, "bytes=1-3");
        embeddedChannel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_get_partial_content_error_thrown_does_not_leak_request() throws Throwable {
        BlobService blobService = mock(BlobService.class);
        when(blobService.getRedirectAddress(anyString(), anyString())).thenReturn(null);
        when(blobService.localBlobShard(anyString(), anyString())).thenThrow(new RuntimeException("dummy"));

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        req.headers().set(io.netty.handler.codec.http.HttpHeaderNames.RANGE, "bytes=1-3");
        EmbeddedChannel channel = createEmbeddedChannel(blobService);
        try {
            channel.writeInbound(req);
        } catch (Exception ignored) {

        } finally {
            assertThat(req.refCnt()).isEqualTo(0);
        }
    }

    @Test
    public void test_get_full_content_happy_path_does_not_leak_request() throws Throwable {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        embeddedChannel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_get_full_content_error_thrown_does_not_leak_request() throws Throwable {
        BlobService blobService = mock(BlobService.class);
        when(blobService.getRedirectAddress(anyString(), anyString())).thenReturn(null);
        when(blobService.localBlobShard(anyString(), anyString())).thenThrow(new RuntimeException("dummy"));

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, BLOB_URI);
        EmbeddedChannel channel = createEmbeddedChannel(blobService);
        try {
            channel.writeInbound(req);
        } catch (Exception ignored) {

        } finally {
            assertThat(req.refCnt()).isEqualTo(0);
        }
    }

    @Test
    public void test_head_not_found_does_not_leak_request() throws Throwable {
        BlobService blobService = mock(BlobService.class);
        when(blobService.getRedirectAddress(anyString(), anyString())).thenReturn(null);
        BlobShard blobShard = mock(BlobShard.class, Answers.RETURNS_DEEP_STUBS);
        when(blobService.localBlobShard(anyString(), anyString())).thenReturn(blobShard);
        when(blobShard.blobContainer().getFile(anyString()).length()).thenReturn(0L);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, BLOB_URI);
        EmbeddedChannel channel = createEmbeddedChannel(blobService);
        channel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_head_status_ok_does_not_leak_request() throws Throwable {
        BlobService blobService = mock(BlobService.class);
        when(blobService.getRedirectAddress(anyString(), anyString())).thenReturn(null);
        BlobShard blobShard = mock(BlobShard.class, Answers.RETURNS_DEEP_STUBS);
        when(blobService.localBlobShard(anyString(), anyString())).thenReturn(blobShard);
        when(blobShard.blobContainer().getFile(anyString()).length()).thenReturn(1L);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, BLOB_URI);
        EmbeddedChannel channel = createEmbeddedChannel(blobService);
        channel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_put_last_chunk_does_not_leak_request() throws Throwable {
        RemoteDigestBlob blob = mock(RemoteDigestBlob.class);
        when(blobService.newBlob(anyString(), anyString())).thenReturn(blob);
        when(blob.addContent(any(), eq(true))).thenReturn(RemoteDigestBlob.Status.FULL);

        // Imitate handleBlobRequest with null content - get headers and instantiate currentMessage
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, BLOB_URI);
        req.headers().set(HttpHeaderNames.EXPECT, HttpHeaderValues.CONTINUE);
        embeddedChannel.writeInbound(req);

        var lastChunk = new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        embeddedChannel.writeInbound(lastChunk);

        assertThat(req.refCnt()).isEqualTo(0);
        assertThat(lastChunk.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_put_chunk_returns_error_does_not_leak_request() throws Throwable {
        RemoteDigestBlob blob = mock(RemoteDigestBlob.class);
        when(blobService.newBlob(anyString(), anyString())).thenReturn(blob);
        when(blob.addContent(any(), anyBoolean())).thenReturn(RemoteDigestBlob.Status.FAILED);

        // Imitate handleBlobRequest with null content - get headers and instantiate currentMessage
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, BLOB_URI);
        req.headers().set(HttpHeaderNames.EXPECT, HttpHeaderValues.CONTINUE);
        embeddedChannel.writeInbound(req);

        var notLastChunk = new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        embeddedChannel.writeInbound(notLastChunk);

        assertThat(req.refCnt()).isEqualTo(0);
        assertThat(notLastChunk.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_put_not_last_chunk_does_not_leak_request() throws Throwable {
        RemoteDigestBlob blob = mock(RemoteDigestBlob.class);
        when(blobService.newBlob(anyString(), anyString())).thenReturn(blob);
        when(blob.addContent(any(), eq(false))).thenReturn(RemoteDigestBlob.Status.PARTIAL);

        // Imitate handleBlobRequest with null content  - get headers and instantiate currentMessage
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, BLOB_URI);
        embeddedChannel.writeInbound(req);

        var notLastChunk = new DefaultHttpContent(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        embeddedChannel.writeInbound(notLastChunk);

        assertThat(req.refCnt()).isEqualTo(0);
        assertThat(notLastChunk.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_delete_success_does_not_leak_request() throws Throwable {
        RemoteDigestBlob blob = mock(RemoteDigestBlob.class);
        when(blobService.newBlob(anyString(), anyString())).thenReturn(blob);
        when(blob.delete()).thenReturn(true);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, BLOB_URI);
        embeddedChannel.writeInbound(req);

        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_delete_not_found_does_not_leak_request() throws Throwable {
        RemoteDigestBlob blob = mock(RemoteDigestBlob.class);
        when(blobService.newBlob(anyString(), anyString())).thenReturn(blob);
        when(blob.delete()).thenReturn(false);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, BLOB_URI);
        embeddedChannel.writeInbound(req);

        assertThat(req.refCnt()).isEqualTo(0);
    }

    @Test
    public void test_unsupported_method_does_not_leak_request() throws Throwable {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PATCH, BLOB_URI);
        embeddedChannel.writeInbound(req);
        assertThat(req.refCnt()).isEqualTo(0);
    }

    private EmbeddedChannel createEmbeddedChannel(BlobService blobService) {
        Role crate = Role.CRATE_USER;
        var sessionSettings = new CoordinatorSessionSettings(crate);
        var mockedSessions = new Sessions(null, null, null, () -> null, null, Settings.EMPTY, clusterService, null) {
            @Override
            public Session newSession(ConnectionProperties connectionProperties, @Nullable String defaultSchema, Role authenticatedUser) {
                return new Session(
                    111,
                    connectionProperties,
                    null,
                    null,
                    null,
                    false,
                    null,
                    sessionSettings,
                    () -> {},
                    1,
                    100);
            }
        };

        return new EmbeddedChannel(new HttpBlobHandler(
            blobService,
            Settings.EMPTY,
            mockedSessions,
            () -> List.of(Role.CRATE_USER)
        ));
    }
}
