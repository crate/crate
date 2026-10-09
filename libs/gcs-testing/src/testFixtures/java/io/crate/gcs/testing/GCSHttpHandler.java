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
package io.crate.gcs.testing;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * Minimal HTTP handler that fakes a GCS JSON API server.
 * <p>
 * Handles the four endpoints OpenDAL's "gcs" backend uses for COPY operations:
 * <ul>
 *   <li>GET  /storage/v1/b/{bucket}/o?prefix=...       — list objects</li>
 *   <li>GET  /storage/v1/b/{bucket}/o/{path}            — stat (object metadata)</li>
 *   <li>GET  /storage/v1/b/{bucket}/o/{path}?alt=media  — read (object content)</li>
 *   <li>POST /upload/storage/v1/b/{bucket}/o?uploadType=media&amp;name={path} — write</li>
 * </ul>
 * <p>
 * Multipart XML API endpoints (/{bucket}/{path}?uploads) are rejected with 501.
 */
public class GCSHttpHandler implements HttpHandler {

    private static final Logger LOGGER = LogManager.getLogger(GCSHttpHandler.class);

    private static final String STORAGE_API_PREFIX = "/storage/v1/b/";
    private static final String UPLOAD_API_PREFIX = "/upload/storage/v1/b/";

    private final Map<String, byte[]> objects;
    private final String bucket;

    public GCSHttpHandler(String bucket) {
        this.bucket = Objects.requireNonNull(bucket);
        this.objects = new ConcurrentHashMap<>();
    }

    public Map<String, byte[]> objects() {
        return objects;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getRawPath();
        String query = exchange.getRequestURI().getRawQuery();
        try (exchange) {
            if ("GET".equals(method) && path.equals(STORAGE_API_PREFIX + bucket + "/o") && query != null) {
                Map<String, String> params = decodeQueryString(exchange.getRequestURI().toString());
                if ("media".equals(params.get("alt"))) {
                    sendError(exchange, 400, "Use /storage/v1/b/{bucket}/o/{path}?alt=media for reads");
                    return;
                }
                handleList(exchange, params);

            } else if ("GET".equals(method) && path.startsWith(STORAGE_API_PREFIX + bucket + "/o/")) {
                String objectName = extractObjectName(path, STORAGE_API_PREFIX + bucket + "/o/");
                Map<String, String> params = query != null
                    ? decodeQueryString(exchange.getRequestURI().toString())
                    : Map.of();
                if ("media".equals(params.get("alt"))) {
                    handleRead(exchange, objectName);
                } else {
                    handleStat(exchange, objectName);
                }

            } else if ("POST".equals(method) && path.startsWith(UPLOAD_API_PREFIX + bucket + "/o")) {
                Map<String, String> params = decodeQueryString(exchange.getRequestURI().toString());
                handleWrite(exchange, params);

            } else if ("DELETE".equals(method) && path.startsWith(STORAGE_API_PREFIX + bucket + "/o/")) {
                String objectName = extractObjectName(path, STORAGE_API_PREFIX + bucket + "/o/");
                objects.remove(objectName);
                exchange.sendResponseHeaders(204, -1);

            } else if (query != null && (query.contains("uploads") || query.contains("uploadId") || query.contains("partNumber"))) {
                sendError(exchange, 501, "Multipart XML API not implemented in test fake");

            } else {
                sendError(exchange, 400, "Unrecognized request: " + method + " " + path);
            }
        } catch (Throwable t) {
            LOGGER.error("GCSHttpHandler error for {} {}", method, path, t);
        }
    }

    private void handleList(HttpExchange exchange, Map<String, String> params) throws IOException {
        String prefix = params.get("prefix");

        StringBuilder json = new StringBuilder();
        json.append("{\"items\":[");
        boolean first = true;
        for (Map.Entry<String, byte[]> entry : objects.entrySet()) {
            String name = entry.getKey();
            if (prefix != null && !name.startsWith(prefix)) {
                continue;
            }
            if (!first) {
                json.append(",");
            }
            first = false;
            appendObjectJson(json, name, entry.getValue().length);
        }
        json.append("],\"prefixes\":[]}");

        byte[] response = json.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
    }

    private void handleStat(HttpExchange exchange, String objectName) throws IOException {
        byte[] data = objects.get(objectName);
        if (data == null) {
            sendError(exchange, 404, "Not Found");
            return;
        }

        StringBuilder json = new StringBuilder();
        json.append("{");
        appendObjectFields(json, objectName, data.length);
        json.append("}");

        byte[] response = json.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
    }

    private void handleRead(HttpExchange exchange, String objectName) throws IOException {
        byte[] data = objects.get(objectName);
        if (data == null) {
            sendError(exchange, 404, "Not Found");
            return;
        }
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(200, data.length);
        exchange.getResponseBody().write(data);
    }

    private void handleWrite(HttpExchange exchange, Map<String, String> params) throws IOException {
        String uploadType = params.get("uploadType");
        if (!"media".equals(uploadType)) {
            sendError(exchange, 501, "Only uploadType=media is supported in test fake, got: " + uploadType);
            return;
        }
        String name = params.get("name");
        if (name == null) {
            sendError(exchange, 400, "Missing 'name' query parameter");
            return;
        }
        name = URLDecoder.decode(name, StandardCharsets.UTF_8);

        byte[] body = exchange.getRequestBody().readAllBytes();
        objects.put(name, body);

        StringBuilder json = new StringBuilder();
        json.append("{");
        appendObjectFields(json, name, body.length);
        json.append("}");
        byte[] response = json.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
    }

    private void appendObjectJson(StringBuilder json, String name, int size) {
        json.append("{");
        appendObjectFields(json, name, size);
        json.append("}");
    }

    private void appendObjectFields(StringBuilder json, String name, int size) {
        String now = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(Instant.now().atOffset(ZoneOffset.UTC));
        json.append("\"name\":\"").append(escapeJson(name)).append("\",");
        json.append("\"size\":\"").append(size).append("\",");
        json.append("\"etag\":\"\\\"test-etag\\\"\",");
        json.append("\"md5Hash\":\"1B2M2Y8AsgTpgAmY7PhCfg==\",");
        json.append("\"updated\":\"").append(now).append("\",");
        json.append("\"contentType\":\"application/octet-stream\"");
    }

    private static String extractObjectName(String fullPath, String prefix) {
        String encoded = fullPath.substring(prefix.length());
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void sendError(HttpExchange exchange, int status, String message) throws IOException {
        byte[] response = ("{\"error\":{\"code\":" + status + ",\"message\":\"" + escapeJson(message) + "\"}}").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        exchange.getResponseBody().write(response);
    }

    private static Map<String, String> decodeQueryString(String uri) {
        var result = new HashMap<String, String>();
        for (var entry : new QueryStringDecoder(uri).parameters().entrySet()) {
            result.put(entry.getKey(), entry.getValue().getFirst());
        }
        return result;
    }
}
