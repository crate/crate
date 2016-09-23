/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.client;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import io.crate.action.sql.SQLBulkRequest;
import io.crate.action.sql.SQLBulkResponse;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public abstract class CrateClientIntegrationTest extends Assert {

    private static final String SQL_REQUEST_TIMEOUT_ENV = "CRATE_TESTS_SQL_REQUEST_TIMEOUT";
    protected static final TimeValue SQL_REQUEST_TIMEOUT = new TimeValue(Long.parseLong(
        MoreObjects.firstNonNull(System.getenv(SQL_REQUEST_TIMEOUT_ENV), "5")), TimeUnit.SECONDS);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static Path distributionsDir = Paths.get(System.getProperty("project_root") + "/app/build/distributions");

    private static String tarBallPath = null;

    static {
        try {
            tarBallPath = Files.newDirectoryStream(distributionsDir, "crate-*.tar.gz").iterator().next()
                .toAbsolutePath().toString();
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        assert tarBallPath != null;
    }

    protected CrateClient client;


    @ClassRule
    public static final CrateTestCluster testCluster = CrateTestCluster.fromFile(tarBallPath)
        .workingDir(Paths.get(System.getProperty("project_build_dir"), "crate-testing")).numberOfNodes(1).build();

    protected String serverAddress() {
        CrateTestServer server = testCluster.randomServer();
        return server.crateHost() + ':' + server.transportPort();
    }

    protected CrateClient client() {
        assert client != null : "client not set";
        return client;
    }

    private void waitForZeroCount(String stmt) {
        for (int i = 1; i < 10; i++) {
            SQLResponse r = client().sql(stmt).actionGet(5, TimeUnit.SECONDS);
            if (((Long) r.rows()[0][0]) == 0L) {
                return;
            }
            try {
                Thread.sleep(i * 100);
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }
        throw new RuntimeException("waiting for zero result timed out");
    }

    protected void ensureYellow() {
        waitForZeroCount("select count(*) from sys.shards where \"primary\" = true and state <> 'STARTED'");
    }

    protected void ensureGreen() {
        waitForZeroCount("select count(*) from sys.shards where state <> 'STARTED'");
    }

    protected SQLResponse execute(String stmt) {
        return client().sql(stmt).actionGet(SQL_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    protected SQLResponse execute(SQLRequest request) {
        return client().sql(request).actionGet(SQL_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    protected SQLBulkResponse execute(String stmt, Object[][] bulkArgs) {
        return client().bulkSql(new SQLBulkRequest(stmt, bulkArgs)).actionGet(SQL_REQUEST_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }
}
