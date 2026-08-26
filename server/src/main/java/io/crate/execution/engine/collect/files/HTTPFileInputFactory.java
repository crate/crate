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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.concurrent.Executor;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.exceptions.Exceptions;

public class HTTPFileInputFactory implements FileInputFactory {

    public static final List<String> NAMES = List.of("http", "https");
    private final HttpClient client;

    @Inject
    public HTTPFileInputFactory(ThreadPool threadPool) {
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
        this.client = HttpClient.newBuilder()
            .executor(executor)
            .followRedirects(Redirect.NORMAL)
            .build();
    }

    @Override
    public FileInput create(URI uri, Settings withClauseOptions) throws IOException {
        return new HTTPFileInput(client, uri);
    }

    static class HTTPFileInput implements FileInput {

        private final URI uri;
        private final HttpClient client;

        public HTTPFileInput(HttpClient client, URI uri) {
            this.client = client;
            this.uri = uri;
        }

        @Override
        public List<URI> expandUri() throws IOException {
            return List.of(uri);
        }

        @Override
        public InputStream getStream(URI uri) throws IOException {
            HttpRequest request = HttpRequest.newBuilder(uri).build();
            try {
                return client.send(request, BodyHandlers.ofInputStream()).body();
            } catch (InterruptedException e) {
                throw Exceptions.toRuntimeException(e);
            }
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
