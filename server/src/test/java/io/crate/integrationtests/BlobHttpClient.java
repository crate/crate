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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import io.crate.common.Hex;
import io.crate.test.utils.Blobs;

class BlobHttpClient {

    private final InetSocketAddress address;

    BlobHttpClient(InetSocketAddress address) {
        this.address = address;
    }

    public CloseableHttpResponse put(String table, String body) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String digest = Hex.encodeHexString(Blobs.digest(body));
            String url = Blobs.url(false, address, table + "/" + digest);
            HttpPut httpPut = new HttpPut(url);
            httpPut.setEntity(new StringEntity(body));
            return httpClient.execute(httpPut);
        }
    }
}
