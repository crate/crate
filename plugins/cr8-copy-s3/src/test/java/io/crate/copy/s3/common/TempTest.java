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

package io.crate.copy.s3.common;

import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.copy.s3.S3CopyPlugin;

public class TempTest extends IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(S3CopyPlugin.class);
        return plugins;
    }

    @Test
    public void test_copy_to_and_copy_from_azure_blob_storage_auth_via_key() throws IOException, InterruptedException {
        execute("CREATE TABLE t1 (x int)");
        execute("INSERT INTO t1 (x) VALUES (1), (2), (3)");
        execute("REFRESH TABLE t1");

        execute("""
                    COPY t1 TO DIRECTORY 's3://127.0.0.1:9000/my.bucket/' WITH (protocol = 'http')
                """);

        execute("CREATE TABLE target (x int)");
        execute("""
                    COPY target FROM 's3://127.0.0.1:9000/my.bucket/*' WITH (protocol = 'http')
                """);
        execute("REFRESH TABLE target");

        execute("SELECT x FROM target order by x");
        execute("REFRESH TABLE target");
        execute("select x from target order by x");
        assertThat(response).hasRows("1", "2", "3");
    }

}
