/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.copy.gcs;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

import io.crate.gcs.testing.GCSHttpHandler;

public class GCSCopyIntegrationTest extends IntegTestCase {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String PROJECT_ID = "test-project";
    private static final String PRIVATE_KEY_ID = "test-key-id";
    private static final String PRIVATE_KEY = "test-private-key";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_EMAIL = "test@test.iam.gserviceaccount.com";

    private String endpoint;
    private HttpServer httpServer;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(GCSCopyPlugin.class);
        return plugins;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        var handler = new GCSHttpHandler(BUCKET_NAME);
        httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext("/", handler);
        httpServer.start();
        InetSocketAddress address = httpServer.getAddress();
        endpoint = String.format(
            Locale.ENGLISH,
            "http://%s:%d",
            "127.0.0.1",
            address.getPort()
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        httpServer.stop(1);
    }

    @Test
    public void test_copy_to_and_copy_from_gcs() throws IOException, InterruptedException {
        execute("CREATE TABLE source (x int)");
        execute("INSERT INTO source(x) values (1), (2), (3)");
        execute("REFRESH TABLE source");

        String gsUri = String.format(Locale.ENGLISH, "gs://%s/dir", BUCKET_NAME);
        execute("""
            COPY source TO DIRECTORY ?
            WITH (
                project_id = ?,
                private_key_id = ?,
                private_key = ?,
                client_id = ?,
                client_email = ?,
                endpoint = ?
            )
            """,
            new Object[]{gsUri, PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_ID, CLIENT_EMAIL, endpoint}
        );

        execute("CREATE TABLE target (x int)");
        execute("""
            COPY target FROM ?
            WITH (
                project_id = ?,
                private_key_id = ?,
                private_key = ?,
                client_id = ?,
                client_email = ?,
                endpoint = ?
            )
            """,
            new Object[]{gsUri + "/*", PROJECT_ID, PRIVATE_KEY_ID, PRIVATE_KEY, CLIENT_ID, CLIENT_EMAIL, endpoint}
        );

        execute("REFRESH TABLE target");
        execute("select x from target order by x");
        assertThat(response).hasRows("1", "2", "3");
    }
}
