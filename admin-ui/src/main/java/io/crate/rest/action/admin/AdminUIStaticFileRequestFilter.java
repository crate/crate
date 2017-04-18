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

package io.crate.rest.action.admin;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static java.nio.file.Files.readAttributes;
import static org.elasticsearch.rest.RestStatus.*;

/**
 * RestFilter for admin ui requests
 * Serves admin ui files
 *
 * /index.html                                  => serve index.html
 * / && isBrowser                               => serve index.html
 * / && isBrowser && Accept: application/json   => don't serve index.html, continue processing
 * / && !isBrowser                              => don't serve any file, continue processing
 * /static/                                     => serve static file
 */
public class AdminUIStaticFileRequestFilter implements RestHandler {

    private final Environment environment;
    private static final Pattern USER_AGENT_BROWSER_PATTERN = Pattern.compile("(Mozilla|Chrome|Safari|Opera|Android|AppleWebKit)+?[/\\s][\\d.]+");

    @Inject
    public AdminUIStaticFileRequestFilter(Environment environment) {
        this.environment = environment;
    }


//    @Override
//    public void process(RestRequest request, RestChannel channel, NodeClient client, RestFilterChain filterChain) throws IOException {
//        if (request.rawPath().equals("/_plugin/crate-admin")){
//            BytesRestResponse resp = new BytesRestResponse(RestStatus.MOVED_PERMANENTLY, "Admin-UI location moved");
//            resp.addHeader("Location", "/");
//            channel.sendResponse(resp);
//            return;
//        }
//        if (request.rawPath().equals("/index.html") || request.rawPath().startsWith("/static/") || shouldServeFromRoot(request)) {
//            serveSite(request, channel);
//        } else {
//            filterChain.continueProcessing(request, channel, client);
//        }
//    }

    // FIXME handle the request here (see process above)
    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {

    }

    static boolean isBrowser(String headerValue) {
        if (headerValue == null) {
            return false;
        }
        String engine = headerValue.split("\\s+")[0];
        return USER_AGENT_BROWSER_PATTERN.matcher(engine).matches();
    }

    private static boolean shouldServeFromRoot(RestRequest request) {
        return request.rawPath().equals("/") && isBrowser(request.header("user-agent")) &&
               !isAcceptJson(request.header("accept"));
    }

    static boolean isAcceptJson(String headerValue) {
        if (headerValue == null) {
            return false;
        }
        return headerValue.contains("application/json");
    }

    private void serveSite(RestRequest request, RestChannel channel) throws IOException {
        if (request.method() != RestRequest.Method.GET) {
            channel.sendResponse(new BytesRestResponse(FORBIDDEN, "GET is the only allowed method"));
            return;
        }
        String sitePath = request.rawPath();
        while (sitePath.length() > 0 && sitePath.charAt(0) == '/') {
            sitePath = sitePath.substring(1);
        }

        // we default to index.html, or what the plugin provides (as a unix-style path)
        // this is a relative path under _site configured by the plugin.
        if (sitePath.length() == 0) {
            sitePath = "index.html";
        }
        final Path siteFile = environment.pluginsFile().resolve("crate-admin").resolve("_site");

        final String separator = siteFile.getFileSystem().getSeparator();
        // Convert file separators.
        sitePath = sitePath.replace("/", separator);
        Path file = siteFile.resolve(sitePath);

        // return not found instead of forbidden to prevent malicious requests to find out if files exist or don't exist
        if (!Files.exists(file) || FileSystemUtils.isHidden(file) ||
            !file.toAbsolutePath().normalize().startsWith(siteFile.toAbsolutePath().normalize())) {
            final String msg = "Requested file [" + file + "] was not found";
            channel.sendResponse(new BytesRestResponse(NOT_FOUND, msg));
            return;
        }

        BasicFileAttributes attributes = readAttributes(file, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            // If it's not a regular file, we send a 403
            final String msg = "Requested file [" + file + "] is not a valid file.";
            channel.sendResponse(new BytesRestResponse(FORBIDDEN, msg));
            return;
        }

        try {
            byte[] data = Files.readAllBytes(file);
            channel.sendResponse(new BytesRestResponse(OK, guessMimeType(file.toAbsolutePath().toString()), data));
        } catch (IOException e) {
            channel.sendResponse(new BytesRestResponse(INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    private static String guessMimeType(String path) {
        int lastDot = path.lastIndexOf('.');
        if (lastDot == -1) {
            return "";
        }
        String extension = path.substring(lastDot + 1).toLowerCase(Locale.ROOT);
        String mimeType = DEFAULT_MIME_TYPES.get(extension);
        if (mimeType == null) {
            return "";
        }
        return mimeType;
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
