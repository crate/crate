/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class SQLHttpIntegrationTest extends SQLTransportIntegrationTest {

    protected CloseableHttpClient httpClient = HttpClients.createDefault();

    protected CloseableHttpResponse post(String body) throws IOException {
        HttpPost httpPost = new HttpPost("http://localhost:44200/_sql?error_trace");

        if(body != null){
            StringEntity bodyEntity = new StringEntity(body);
            httpPost.setEntity(bodyEntity);
        }
        CloseableHttpResponse response = httpClient.execute(httpPost);
        return response;
    }

    protected CloseableHttpResponse post() throws IOException {
        return post(null);
    }

}
