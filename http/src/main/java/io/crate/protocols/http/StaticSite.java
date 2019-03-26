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

package io.crate.protocols.http;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.io.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.Map;

import static java.nio.file.Files.readAttributes;

public final class StaticSite {

    public static FullHttpResponse serveSite(Path siteDirectory,
                                             FullHttpRequest request,
                                             ByteBufAllocator alloc) throws IOException {
        if (request.method() != HttpMethod.GET) {
            return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FORBIDDEN);
        }
        String sitePath = request.uri();
        while (sitePath.length() > 0 && sitePath.charAt(0) == '/') {
            sitePath = sitePath.substring(1);
        }

        // we default to index.html, or what the plugin provides (as a unix-style path)
        // this is a relative path under _site configured by the plugin.
        if (sitePath.length() == 0) {
            sitePath = "index.html";
        }

        final String separator = siteDirectory.getFileSystem().getSeparator();
        // Convert file separators.
        sitePath = sitePath.replace("/", separator);
        Path file = siteDirectory.resolve(sitePath);

        // return not found instead of forbidden to prevent malicious requests to find out if files exist or don't exist
        if (!Files.exists(file) || FileSystemUtils.isHidden(file) ||
            !file.toAbsolutePath().normalize().startsWith(siteDirectory.toAbsolutePath().normalize())) {

            return Responses.contentResponse(
                HttpResponseStatus.NOT_FOUND, alloc, "Requested file [" + file + "] was not found");
        }

        BasicFileAttributes attributes = readAttributes(file, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            // If it's not a regular file, we send a 403
            final String msg = "Requested file [" + file + "] is not a valid file.";
            return Responses.contentResponse(HttpResponseStatus.NOT_FOUND, alloc, msg);
        }
        try {
            byte[] data = Files.readAllBytes(file);
            var resp = Responses.contentResponse(HttpResponseStatus.OK, alloc, data);
            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, guessMimeType(file.toAbsolutePath().toString()));
            return resp;
        } catch (IOException e) {
            return Responses.contentResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, alloc, e.getMessage());
        }
    }

    private static String guessMimeType(String path) {
        int lastDot = path.lastIndexOf('.');
        if (lastDot == -1) {
            return "";
        }
        String extension = path.substring(lastDot + 1).toLowerCase(Locale.ROOT);
        return DEFAULT_MIME_TYPES.getOrDefault(extension, "");
    }

    private static final Map<String, String> DEFAULT_MIME_TYPES = new ImmutableMap.Builder<String, String>()
        .put("txt", "text/plain")
        .put("css", "text/css")
        .put("csv", "text/csv")
        .put("htm", "text/html")
        .put("html", "text/html")
        .put("xml", "text/xml")
        .put("js", "text/javascript") // Technically it should be application/javascript (RFC 4329), but IE8 struggles with that
        .put("xhtml", "application/xhtml+xml")
        .put("json", "application/json")
        .put("pdf", "application/pdf")
        .put("zip", "application/zip")
        .put("tar", "application/x-tar")
        .put("gif", "image/gif")
        .put("jpeg", "image/jpeg")
        .put("jpg", "image/jpeg")
        .put("tiff", "image/tiff")
        .put("tif", "image/tiff")
        .put("png", "image/png")
        .put("svg", "image/svg+xml")
        .put("ico", "image/vnd.microsoft.icon")
        .put("mp3", "audio/mpeg")
        .build();
}
