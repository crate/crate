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

package org.elasticsearch.repositories.azure;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

import io.crate.azure.testing.AzureHttpHandler;
import io.crate.testing.Asserts;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST)
public class AzureSnapshotIntegrationTest extends IntegTestCase {

    private static final String CONTAINER_NAME = "crate_snapshots";

    private HttpServer httpServer;
    private AzureHttpHandler handler;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AzureRepositoryPlugin.class);
        return plugins;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        handler = new AzureHttpHandler(CONTAINER_NAME, false);
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/" + CONTAINER_NAME, handler);
        httpServer.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        httpServer.stop(1);
        super.tearDown();
    }

    @Test
    public void create_azure_snapshot_and_restore_with_endpoint_suffix() {
        execute("CREATE TABLE t1 (x int)");
        assertThat(response.rowCount()).isEqualTo(1L);

        int numberOfDocs = randomIntBetween(0, 10);
        Object[][] rows = new Object[numberOfDocs][];
        for (int i = 0; i < numberOfDocs; i++) {
            rows[i] = new Object[] { randomInt() };
        }
        execute("INSERT INTO t1 (x) VALUES (?)", rows);
        execute("REFRESH TABLE t1");

        execute("CREATE REPOSITORY r1 TYPE AZURE WITH (" +
                "container = '" + CONTAINER_NAME + "', " +
                "account = 'devstoreaccount1', " +
                "key = 'ZGV2c3RvcmVhY2NvdW50MQ==', " +
                "endpoint_suffix = 'ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + "')");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("CREATE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)");

        execute("DROP TABLE t1");

        execute("RESTORE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)");
        execute("REFRESH TABLE t1");

        execute("SELECT COUNT(*) FROM t1");
        assertThat(response.rows()[0][0]).isEqualTo((long) numberOfDocs);

        execute("DROP SNAPSHOT r1.s1");
        handler.blobs().keySet().forEach(x -> assertThat(x).doesNotEndWith("dat"));

        execute("SELECT * FROM sys.repositories");
        assertThat(response.rows()[0][0]).isEqualTo("r1");
        //noinspection unchecked
        assertThat((Map<String, String>) response.rows()[0][1])
            .hasEntrySatisfying("account", v -> assertThat(v).isEqualTo("[xxxxx]"))
            .hasEntrySatisfying("container", v -> assertThat(v).isEqualTo("crate_snapshots"))
            .hasEntrySatisfying("endpoint_suffix", v -> assertThat(v).matches("ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:\\d+"))
            .hasEntrySatisfying("key", v -> assertThat(v).isEqualTo("[xxxxx]"));
        assertThat(response.rows()[0][2]).isEqualTo("azure");
    }

    @Test
    public void create_azure_snapshot_and_restore_with_secondary_endpoint() {
        execute("CREATE TABLE t1 (x int)");
        assertThat(response.rowCount()).isEqualTo(1L);

        int numberOfDocs = randomIntBetween(0, 10);
        Object[][] rows = new Object[numberOfDocs][];
        for (int i = 0; i < numberOfDocs; i++) {
            rows[i] = new Object[] { randomInt() };
        }
        execute("INSERT INTO t1 (x) VALUES (?)", rows);
        execute("REFRESH TABLE t1");

        execute("CREATE REPOSITORY r1 TYPE AZURE WITH (" +
                "container = '" + CONTAINER_NAME + "', " +
                "account = 'devstoreaccount1', " +
                "key = 'ZGV2c3RvcmVhY2NvdW50MQ==', " +
                "location_mode = 'PRIMARY_ONLY', " +
                "endpoint = '" + httpServerUrl() + "')");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("CREATE SNAPSHOT r1.s1 ALL WITH (wait_for_completion = true)");

        execute("DROP TABLE t1");

        // secondary endpoint is by read-only
        execute("CREATE REPOSITORY r2 TYPE AZURE WITH (" +
                "container = '" + CONTAINER_NAME + "', " +
                "account = 'devstoreaccount1', " +
                "key = 'ZGV2c3RvcmVhY2NvdW50MQ==', " +
                "location_mode = 'SECONDARY_ONLY', " +
                "endpoint = '" + invalidHttpServerUrl() + "', "+
                "secondary_endpoint = '" + httpServerUrl() + "')");

        execute("RESTORE SNAPSHOT r2.s1 ALL WITH (wait_for_completion = true)");
        execute("REFRESH TABLE t1");

        execute("SELECT COUNT(*) FROM t1");
        assertThat(response.rows()[0][0]).isEqualTo((long) numberOfDocs);

        execute("DROP SNAPSHOT r1.s1");
        handler.blobs().keySet().forEach(x -> assertThat(x).doesNotEndWith("dat"));
    }

    @Test
    public void test_create_azure_repo_with_sas_token() {
        execute("CREATE REPOSITORY r1 TYPE AZURE WITH (" +
            "container = '" + CONTAINER_NAME + "', " +
            "account = 'devstoreaccount1', " +
            "sas_token = 'aaQc3RvcmVhY2NvdW50Mdhdhd', " +
            "location_mode = 'PRIMARY_ONLY', " +
            "endpoint = '" + httpServerUrl() + "')");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("SELECT * FROM sys.repositories");
        assertThat(response.rows()[0][0]).isEqualTo("r1");
        //noinspection unchecked
        assertThat((Map<String, String>) response.rows()[0][1])
            .hasEntrySatisfying("account", v -> assertThat(v).isEqualTo("[xxxxx]"))
            .hasEntrySatisfying("container", v -> assertThat(v).isEqualTo("crate_snapshots"))
            .hasEntrySatisfying("endpoint", v -> assertThat(v).matches("http://127.0.0.1:\\d+"))
            .hasEntrySatisfying("location_mode", v -> assertThat(v).isEqualTo("PRIMARY_ONLY"))
            .hasEntrySatisfying("sas_token", v -> assertThat(v).isEqualTo("[xxxxx]"));
        assertThat(response.rows()[0][2]).isEqualTo("azure");
    }

    @Test
    public void test_create_azure_repo_with_sas_token_and_key_fails() {
        Asserts.assertSQLError(() ->
            execute("CREATE REPOSITORY r1 TYPE AZURE WITH (" +
            "container = '" + CONTAINER_NAME + "', " +
            "account = 'devstoreaccount1', " +
            "sas_token = 'aaQc3RvcmVhY2NvdW50Mdhdhd', " +
            "key = 'ZGV2c3RvcmVhY2NvdW50MQ==', " +
            "location_mode = 'PRIMARY_ONLY', " +
            "endpoint = '" + httpServerUrl() + "')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("[r1] Unable to verify the repository, [r1] is not accessible on master node: " +
                "SettingsException 'Both a secret as well as a shared access token were set.'");
    }

    @Test
    public void test_create_azure_repo_with_missing_mandatory_settings() {
        Asserts.assertSQLError(() ->
                execute("CREATE REPOSITORY r1 TYPE AZURE WITH (account = 'devstoreaccount1')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("[r1] Unable to verify the repository, [r1] is not accessible on master node: " +
                "SettingsException 'Neither a secret key nor a shared access token was set.'");
    }

    @Test
    public void test_invalid_settings_to_create_azure_repository() {
        Asserts.assertSQLError(() -> execute(
            "CREATE REPOSITORY r1 TYPE AZURE WITH (container = 'invalid', " +
            "account = 'devstoreaccount1', " +
            "key = 'ZGV2c3RvcmVhY2NvdW50MQ=='," +
            "endpoint_suffix = 'ignored;DefaultEndpointsProtocol=http;BlobEndpoint')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("[r1] Unable to verify the repository, [r1] is not accessible on master node: " +
                                         "IllegalArgumentException 'Invalid connection string.'");
    }

    private String httpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + address.getAddress().getHostAddress() + ":" + address.getPort();
    }

    private String invalidHttpServerUrl() {
        InetSocketAddress address = httpServer.getAddress();
        return "http://127.0.0.0:" + address.getPort();
    }
}
