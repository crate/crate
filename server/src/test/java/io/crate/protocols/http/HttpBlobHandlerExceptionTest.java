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

package io.crate.protocols.http;

import io.crate.blob.BlobService;
import io.crate.blob.exceptions.DigestMismatchException;
import io.crate.blob.exceptions.DigestNotFoundException;
import io.crate.blob.v2.BlobsDisabledException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.NotSslRecordException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpBlobHandlerExceptionTest {

    @Mock
    private ChannelHandlerContext ctx;
    
    @Mock
    private ChannelPipeline pipeline;

    @Mock
    private BlobService blobService;

    @Mock
    private DiscoveryNode discoveryNode;

    @Mock
    private Channel channel;

    private HttpBlobHandler blobHandler;
    private EmbeddedChannel embeddedChannel;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Netty4CorsConfig corsConfig = Netty4CorsConfigBuilder.forAnyOrigin().build();
        blobHandler = new HttpBlobHandler(blobService, corsConfig);
        embeddedChannel = new EmbeddedChannel(blobHandler);
        
        // Setup channel mock
        when(ctx.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
    }

    @Test
    public void testBlobSpecificExceptionsAreHandled() throws Exception {
        // Test DigestMismatchException
        blobHandler.exceptionCaught(ctx, new DigestMismatchException("expected", "actual"));
        verify(ctx, never()).fireExceptionCaught(new DigestMismatchException("expected", "actual"));

        // Test BlobsDisabledException
        Index index = new Index("test", "uuid");
        blobHandler.exceptionCaught(ctx, new BlobsDisabledException(index));
        verify(ctx, never()).fireExceptionCaught(new BlobsDisabledException(index));

        // Test DigestNotFoundException
        blobHandler.exceptionCaught(ctx, new DigestNotFoundException("test"));
        verify(ctx, never()).fireExceptionCaught(new DigestNotFoundException("test"));
    }

    @Test
    public void testNonBlobExceptionsArePropagated() throws Exception {
        // Test with a non-blob exception
        RuntimeException testException = new RuntimeException("test");
        blobHandler.exceptionCaught(ctx, testException);
        verify(ctx).fireExceptionCaught(testException);

        // Test with IndexNotFoundException
        IndexNotFoundException indexException = new IndexNotFoundException("test");
        blobHandler.exceptionCaught(ctx, indexException);
        verify(ctx).fireExceptionCaught(indexException);

        // Test with IllegalArgumentException
        IllegalArgumentException illegalArgException = new IllegalArgumentException("test");
        blobHandler.exceptionCaught(ctx, illegalArgException);
        verify(ctx).fireExceptionCaught(illegalArgException);

        // Test with EsRejectedExecutionException
        EsRejectedExecutionException rejectedException = new EsRejectedExecutionException("test", false);
        blobHandler.exceptionCaught(ctx, rejectedException);
        verify(ctx).fireExceptionCaught(rejectedException);
    }

    @Test
    public void testConnectionExceptionsAreHandled() throws Exception {
        // Test ClosedChannelException
        blobHandler.exceptionCaught(ctx, new java.nio.channels.ClosedChannelException());
        verify(ctx, never()).fireExceptionCaught(new java.nio.channels.ClosedChannelException());

        // Test IOException with "Connection reset by peer"
        java.io.IOException resetException = new java.io.IOException("Connection reset by peer");
        blobHandler.exceptionCaught(ctx, resetException);
        verify(ctx, never()).fireExceptionCaught(resetException);

        // Test NotSslRecordException
        NotSslRecordException sslException = new NotSslRecordException("test");
        blobHandler.exceptionCaught(ctx, sslException);
        verify(ctx, never()).fireExceptionCaught(sslException);
    }

    @Test
    public void testExceptionHandlingWithHttpRequest() throws Exception {
        // Create a test HTTP request
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "/_blobs/test/123"
        );

        // Test blob-specific exception with an active request
        embeddedChannel.writeInbound(request);
        blobHandler.exceptionCaught(ctx, new DigestMismatchException("expected", "actual"));
        verify(ctx, never()).fireExceptionCaught(new DigestMismatchException("expected", "actual"));

        // Test non-blob exception with an active request
        embeddedChannel.writeInbound(request);
        IndexNotFoundException indexException = new IndexNotFoundException("test");
        blobHandler.exceptionCaught(ctx, indexException);
        verify(ctx).fireExceptionCaught(indexException);
    }
} 