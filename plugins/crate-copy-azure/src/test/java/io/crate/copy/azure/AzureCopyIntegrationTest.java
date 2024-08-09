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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

public class AzureCopyIntegrationTest extends IntegTestCase {

    private static final String CONTAINER_NAME = "test";
    private static final String AZURITE_ACCOUNT = "devstoreaccount1";
    private static final String AZURITE_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private String endpoint;
    private GenericContainer<?> azureContainer;
    private BlobServiceClient blobServiceClient;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AzureCopyPlugin.class);
        return plugins;
    }

    @Before
    public void setup() {
        azureContainer = new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:3.31.0")
                .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                        new HostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(10000),
                                new ExposedPort(10000)))
                ));
        azureContainer.start();
        String host = azureContainer.getContainerIpAddress();
        Integer port = azureContainer.getMappedPort(10000);
        endpoint = String.format(Locale.ENGLISH, "http://%s:%d/%s", host, port, AZURITE_ACCOUNT);
        String connString = String.format(
                Locale.ENGLISH,
                "AccountName=%s;AccountKey=%s;DefaultEndpointsProtocol=http;BlobEndpoint=%s;",
                AZURITE_ACCOUNT,
                AZURITE_KEY,
                endpoint
        );
        blobServiceClient = new BlobServiceClientBuilder().connectionString(connString).buildClient();
        blobServiceClient.createBlobContainerIfNotExists(CONTAINER_NAME);
    }

    @After
    public void cleanUp() {
        azureContainer.stop();
    }

    @Test
    public void test_copy_to_and_copy_from_azure_blob_storage() {
        execute("CREATE TABLE source (x int)");
        execute("INSERT INTO source(x) values (1), (2), (3)");
        execute("REFRESH TABLE source");

        execute("""
            COPY source TO DIRECTORY 'azblob://dir1/dir2'
            WITH (
                container = ?,
                account_name = ?,
                account_key = ?,
                endpoint = ?
            )
            """,
            new Object[]{CONTAINER_NAME, AZURITE_ACCOUNT, AZURITE_KEY, endpoint}
        );

        execute("CREATE TABLE target (x int)");
        execute("""
            COPY target FROM 'azblob://dir1/dir2/*'
            WITH (
                container = ?,
                account_name = ?,
                account_key = ?,
                endpoint = ?
            )
            """,
            new Object[]{CONTAINER_NAME, AZURITE_ACCOUNT, AZURITE_KEY, endpoint}
        );

        execute("REFRESH TABLE target");
        execute("select x from target order by x");
        assertThat(response).hasRows("1", "2", "3");
    }
}
