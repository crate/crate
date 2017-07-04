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

import com.google.common.collect.ImmutableList;
import io.crate.Build;
import io.crate.Version;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static io.crate.protocols.http.StaticSite.serveSite;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
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
    public static final Setting<Boolean> ES_API_ENABLED_SETTING = Setting.boolSetting(
        "es.api.enabled", false, Setting.Property.NodeScope);

    private static final Pattern USER_AGENT_BROWSER_PATTERN = Pattern.compile("(Mozilla|Chrome|Safari|Opera|Android|AppleWebKit)+?[/\\s][\\d.]+");
    private static final List<String> SUPPORTED_ENDPOINTS = ImmutableList.of(
        "/_sql",
        "/_blobs",
        "/index.html",
        "/static",
        "/admin",
        "/_plugin"
    );

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
                               ClusterService clusterService) {
        this.settings = settings;
        this.controller = controller;
        this.version = Version.CURRENT;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
        siteDirectory = environment.libFile().resolve("site");
        Boolean esApiEnabled = ES_API_ENABLED_SETTING.get(settings);
        Logger logger = Loggers.getLogger(getClass().getPackage().getName(), settings);
        logger.info("Elasticsearch HTTP REST API {}enabled", esApiEnabled ? "" : "not ");
    }

    void registerHandler() {
        controller.registerHandler(GET, PATH, this);
        controller.registerHandler(HEAD, PATH, this);
        controller.registerHandler(GET, "/admin", (req, channel, client) -> redirectToRoot(channel));
        controller.registerHandler(GET, "/_plugin/crate-admin", (req, channel, client) -> redirectToRoot(channel));
        controller.registerHandler(GET, "/index.html", (req, channel, client) -> serveSite(siteDirectory, req, channel));
    }

    private static boolean endpointAllowed(String rawPath) {
        return isRoot(rawPath) || isSupportedEndpoint(rawPath);
    }

    private static boolean isSupportedEndpoint(String rawPath) {
        for (int i = 0; i < SUPPORTED_ENDPOINTS.size(); i++) {
            if (rawPath.startsWith(SUPPORTED_ENDPOINTS.get(i))) {
                return true;
            }
        }
        return false;
    }

    // handle possible (wrong) URL '//' too
    // as some http clients create wrong requests to the ``root`` path '/' with '//'
    // we do handle arbitrary numbers of '/' in the path
    private static boolean isRoot(String rawPath) {
        for (int i = 0; i < rawPath.length(); i++) {
            if (rawPath.charAt(i) != '/') {
                return false;
            }
        }
        return true;
    }


    private static void redirectToRoot(RestChannel channel) {
        BytesRestResponse resp = new BytesRestResponse(RestStatus.TEMPORARY_REDIRECT, "");
        resp.addHeader("Location", "/");
        channel.sendResponse(resp);
    }



    private void serveJSONOrSite(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
        if (shouldServeFromRoot(request)) {
            serveSite(siteDirectory, request, channel);
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

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        serveJSONOrSite(request, channel, client);
    }

    public static class RestFilter implements RestHandler {
        private final RestHandler delegate;
        private final Boolean esApiEnabled;

        public RestFilter(Settings settings, RestHandler delegate) {
            this.esApiEnabled = ES_API_ENABLED_SETTING.get(settings);
            this.delegate = delegate;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            String rawPath = request.rawPath();
            if (esApiEnabled || endpointAllowed(rawPath)) {
                delegate.handleRequest(request, channel, client);
            } else {
                channel.sendResponse(new BytesRestResponse(
                    BAD_REQUEST,
                    String.format(Locale.ENGLISH,
                        "No handler found for uri [%s] and method [%s]",
                        request.uri(),
                        request.method())
                ));
            }
        }
    }
}

