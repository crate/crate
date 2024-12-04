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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import io.crate.common.Hex;
import io.crate.test.utils.Blobs;

class BlobHttpClient {

    private final InetSocketAddress address;

    BlobHttpClient(InetSocketAddress address) {
        this.address = address;
    }

    public HttpResponse<String> put(String table, String body) throws Exception {
        try (var httpClient = HttpClient.newHttpClient()) {
            String digest = Hex.encodeHexString(Blobs.digest(body));
            URI uri = Blobs.url(false, address, table + "/" + digest);
            HttpRequest request = HttpRequest.newBuilder(uri)
                .PUT(BodyPublishers.ofString(body))
                .build();

            return httpClient.send(request, BodyHandlers.ofString());
        }
    }
}
