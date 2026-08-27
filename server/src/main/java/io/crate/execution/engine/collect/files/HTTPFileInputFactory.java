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

package io.crate.execution.engine.collect.files;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executor;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.exceptions.Exceptions;
import io.crate.types.DataTypes;

public class HTTPFileInputFactory implements FileInputFactory {

    public static final List<String> NAMES = List.of("http", "https");
    public static final String LINK_LOCAL = "_link_";
    public static final String SITE_LOCAL = "_site_";
    public static final String LOCAL = "_local_";
    public static final Setting<Redirect> REDIRECT_SETTING = new Setting<>(
        "copy_from.http.redirects",
        "normal",
        value -> switch (value.toLowerCase(Locale.ENGLISH)) {
            case "normal" -> Redirect.NORMAL;
            case "always" -> Redirect.ALWAYS;
            case "never" -> Redirect.NEVER;
            default -> throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "Invalid redirects value '%s'. Expected one of [normal, always, never]",
                    value
                ));
        },
        DataTypes.STRING,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Exposed
    );

    public static final Setting<List<String>> BLOCKED_HOSTS = Setting.listSetting(
        "copy_from.http.blocked_hosts",
        List.of(),
        v -> v,
        DataTypes.STRING_ARRAY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Exposed
    );

    private final ClusterSettings clusterSettings;

    @VisibleForTesting
    volatile HttpClient client;

    @Inject
    public HTTPFileInputFactory(ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.clusterSettings = clusterSettings;
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
        Redirect redirect = clusterSettings.get(REDIRECT_SETTING);
        this.client = HttpClient.newBuilder()
            .executor(executor)
            .followRedirects(redirect)
            .build();

        clusterSettings.addSettingsUpdateConsumer(REDIRECT_SETTING, redirectValue -> {
            this.client = HttpClient.newBuilder()
                .executor(executor)
                .followRedirects(redirectValue)
                .build();
        });
    }

    @Override
    public FileInput create(URI uri, Settings withClauseOptions) throws IOException {
        return new HTTPFileInput(client, uri, clusterSettings.get(BLOCKED_HOSTS));
    }

    static class HTTPFileInput implements FileInput {

        private final URI uri;
        private final HttpClient client;
        private final Set<String> blockedHosts;

        public HTTPFileInput(HttpClient client, URI uri, List<String> blockedHosts) {
            this.client = client;
            this.uri = uri;
            this.blockedHosts = Set.copyOf(blockedHosts);
        }

        @Override
        public List<URI> expandUri() throws IOException {
            return List.of(uri);
        }

        @Override
        public InputStream getStream(URI uri) throws IOException {
            ensureNotBlocked(uri.getHost());
            HttpRequest request = HttpRequest.newBuilder(uri).build();
            try {
                return client.send(request, BodyHandlers.ofInputStream()).body();
            } catch (InterruptedException e) {
                throw Exceptions.toRuntimeException(e);
            }
        }

        private void ensureNotBlocked(String host) throws UnknownHostException {
            if (blockedHosts.contains(host)) {
                raiseBlocked(host);
            }
            for (var addr : InetAddress.getAllByName(host)) {
                String hostAddr = addr.getHostAddress();
                if (blockedHosts.contains(hostAddr)) {
                    raiseBlocked(hostAddr);
                }
                if ((addr.isAnyLocalAddress() || addr.isLoopbackAddress()) && blockedHosts.contains(LOCAL)) {
                    raiseBlocked(hostAddr);
                }
                if (addr.isSiteLocalAddress() && blockedHosts.contains(SITE_LOCAL)) {
                    raiseBlocked(hostAddr);
                }
                if (addr.isLinkLocalAddress() && blockedHosts.contains(LINK_LOCAL)) {
                    raiseBlocked(hostAddr);
                }
            }
        }

        private static void raiseBlocked(String host) {
            throw new IllegalArgumentException("Host `" + host + "` is blocked");
        }

        @Override
        public boolean isGlobbed() {
            return false;
        }

        @Override
        public URI uri() {
            return uri;
        }

        @Override
        public boolean sharedStorageDefault() {
            return true;
        }
    }
}
