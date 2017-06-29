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

package io.crate.rest;

import com.google.common.collect.ImmutableMap;
import io.crate.Build;
import io.crate.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilter;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static java.nio.file.Files.readAttributes;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.FORBIDDEN;
import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * RestAction for root or admin-ui requests
 *
 * /index.html                                  => serve index.html
 * / && isBrowser                               => serve index.html
 * / && isBrowser && Accept: application/json   => serve JSON
 * / && !isBrowser                              => serve JSON
 * /static/                                     => serve static file
 */
public class CrateRestMainAction implements RestHandler {

    public static final String PATH = "/";
    private static final Pattern USER_AGENT_BROWSER_PATTERN = Pattern.compile("(Mozilla|Chrome|Safari|Opera|Android|AppleWebKit)+?[/\\s][\\d.]+");

    private final Version version;
    private final ClusterName clusterName;
    private final ClusterService clusterService;
    private final Settings settings;
    private final RestController controller;
    private final Path siteDirectory;

    @Inject
    public CrateRestMainAction(Settings settings,
                               Environment environment,
                               RestController controller,
                               ClusterService clusterService,
                               CrateRestFilter crateRestFilter) {
        this.settings = settings;
        this.controller = controller;
        this.version = Version.CURRENT;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
        siteDirectory = environment.libFile().resolve("site");
    }

    void registerHandler() {
        controller.registerHandler(GET, PATH, this);
        controller.registerHandler(HEAD, PATH, this);
        controller.registerHandler(GET, "/admin", (req, channel, client) -> redirectToRoot(channel));
        controller.registerHandler(GET, "/_plugin/crate-admin", (req, channel, client) -> redirectToRoot(channel));
        controller.registerHandler(GET, "/index.html", this::serveSite);
        controller.registerHandler(GET, "/static/{file}", this::serveSite);
        controller.registerFilter(new RestFilter() {
            @Override
            public void process(RestRequest request, RestChannel channel, NodeClient client, RestFilterChain filterChain) throws Exception {
                if (request.rawPath().startsWith("/static/")) {
                    serveSite(request, channel, client);
                } else {
                    filterChain.continueProcessing(request, channel, client);
                }
            }
        });
    }

    private static void redirectToRoot(RestChannel channel) {
        BytesRestResponse resp = new BytesRestResponse(RestStatus.TEMPORARY_REDIRECT, "");
        resp.addHeader("Location", "/");
        channel.sendResponse(resp);
    }

    private void serveSite(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
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

        final String separator = siteDirectory.getFileSystem().getSeparator();
        // Convert file separators.
        sitePath = sitePath.replace("/", separator);
        Path file = siteDirectory.resolve(sitePath);

        // return not found instead of forbidden to prevent malicious requests to find out if files exist or don't exist
        if (!Files.exists(file) || FileSystemUtils.isHidden(file) ||
            !file.toAbsolutePath().normalize().startsWith(siteDirectory.toAbsolutePath().normalize())) {
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

    private void serveJSONOrSite(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
        if (shouldServeFromRoot(request)) {
            serveSite(request, channel, client);
        } else {
            serveJSON(request, channel, client);
        }
    }

    private static boolean shouldServeFromRoot(RestRequest request) {
        return request.rawPath().equals("/") && isBrowser(request.header("user-agent")) && !isAcceptJson(request.header("accept"));
    }

    static boolean isBrowser(String headerValue) {
        if (headerValue == null){
            return false;
        }
        String engine = headerValue.split("\\s+")[0];
        return USER_AGENT_BROWSER_PATTERN.matcher(engine).matches();
    }

    static boolean isAcceptJson(String headerValue) {
        return headerValue != null && headerValue.contains("application/json");
    }

    private void serveJSON(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
        final RestStatus status;
        if (clusterService.state().blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
            status = RestStatus.SERVICE_UNAVAILABLE;
        } else {
            status = OK;
        }
        if (request.method() == RestRequest.Method.HEAD) {
            channel.sendResponse(new BytesRestResponse(status, channel.newBuilder()));
        }

        XContentBuilder builder = channel.newBuilder();
        builder.prettyPrint().lfAtEnd();
        builder.startObject();
        builder.field("ok", status.equals(OK));
        builder.field("status", status.getStatus());

        String nodeName = NODE_NAME_SETTING.get(settings);
        if (nodeName != null && !nodeName.isEmpty()) {
            builder.field("name", nodeName);
        }

        builder.field("cluster_name", clusterName.value());
        builder.startObject("version")
            .field("number", version.number())
            .field("build_hash", Build.CURRENT.hash())
            .field("build_timestamp", Build.CURRENT.timestamp())
            .field("build_snapshot", version.snapshot)
            .field("es_version", version.esVersion)
            // We use the lucene version from lucene constants since
            // this includes bugfix release version as well and is already in
            // the right format. We can also be sure that the format is maitained
            // since this is also recorded in lucene segments and has BW compat
            .field("lucene_version", org.apache.lucene.util.Version.LATEST.toString())
            .endObject();
        builder.endObject();
        channel.sendResponse(new BytesRestResponse(status, builder));
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

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        serveJSONOrSite(request, channel, client);
    }
}

