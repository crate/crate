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

package io.crate.copy.azure;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpServer;

import io.crate.azure.testing.AzureHttpHandler;
import io.crate.lucene.CrateLuceneTestCase;

@ThreadLeakFilters(
        defaultFilters = true,
        filters = {
            QuickPatchThreadsFilter.class,
            AzureCopyIntegrationTest.OpenDALFilter.class,
            CrateLuceneTestCase.CommonPoolFilter.class})
public class AzureCopyIntegrationTest extends IntegTestCase {

    private static final String CONTAINER_NAME = "test";
    public static final String AZURE_ACCOUNT = "devstoreaccount1";
    private static final String AZURE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /**
     * Generated via
     * az storage container generate-sas
     * --account-key Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
     * --account-name devstoreaccount1
     * --expiry 2025-01-01 --name test --permissions dlrw
     */
    private static final String SAS_TOKEN =
        "se=2025-01-01&sp=rwdl&sv=2022-11-02&sr=c&sig=E3VL9JiO78xUQsqzBoqDStPliSoGnguHcGvs%2BE9o8h8%3D";

    private String containerUri;
    private HttpServer httpServer;

    /**
     * OpenDAL uses shared singleton async tokio executor with configured number of threads.
     * They are re-used throughout the app lifetime and cleaned up on executor disposal on node shutdown.
     **/
    public static class OpenDALFilter implements ThreadFilter {

        @Override
        public boolean reject(Thread t) {
            // TODO: Used more reliable/less common pattern once https://github.com/apache/opendal/issues/5088 is implemented.
            return t.getName().startsWith("Thread-");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AzureCopyPlugin.class);
        return plugins;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        var handler = new AzureHttpHandler(CONTAINER_NAME, true);
        httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        httpServer.createContext("/" + CONTAINER_NAME , handler);
        httpServer.start();
        InetSocketAddress address = httpServer.getAddress();

        String host = String.format(
            Locale.ENGLISH,
            "%s:%d",
            address.getAddress().getCanonicalHostName(),
            address.getPort()
        );
        containerUri = String.format(
            Locale.ENGLISH,
            "az://%s.%s/%s",
            AZURE_ACCOUNT,
            host,
            CONTAINER_NAME
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        httpServer.stop(1);
    }

    @Test
    public void test_copy_to_and_copy_from_azure_blob_storage_auth_via_key() throws IOException, InterruptedException {
        execute("CREATE TABLE source (x int)");
        execute("INSERT INTO source(x) values (1), (2), (3)");
        execute("REFRESH TABLE source");

        String fullURI = containerUri;
        execute("""
            COPY source TO DIRECTORY ?
            WITH (
                protocol = 'http',
                key = ?
            )
            """,
            new Object[]{fullURI, AZURE_KEY}
        );

        execute("CREATE TABLE target (x int)");
        execute("""
            COPY target FROM ?
            WITH (
                protocol = 'http',
                key = ?
            )
            """,
            new Object[]{fullURI + "/*", AZURE_KEY}
        );

        execute("REFRESH TABLE target");
        execute("select x from target order by x");
        assertThat(response).hasRows("1", "2", "3");
    }

    @Test
    public void test_copy_to_and_copy_from_azure_blob_storage_auth_via_token() throws IOException, InterruptedException {
        execute("CREATE TABLE source (x int)");
        execute("INSERT INTO source(x) values (1), (2), (3)");
        execute("REFRESH TABLE source");

        String fullURI = containerUri + "/dir/dir2";
        execute("""
            COPY source TO DIRECTORY ?
            WITH (
                protocol = 'http',
                sas_token = ?
            )
            """,
            new Object[]{fullURI, SAS_TOKEN}
        );

        execute("CREATE TABLE target (x int)");
        execute("""
            COPY target FROM ?
            WITH (
                protocol = 'http',
                sas_token = ?
            )
            """,
            new Object[]{fullURI + "/*", SAS_TOKEN}
        );

        execute("REFRESH TABLE target");
        execute("select x from target order by x");
        assertThat(response).hasRows("1", "2", "3");
    }
}
