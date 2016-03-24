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

import com.google.common.base.Throwables;
import io.crate.action.sql.SQLResponse;
import io.crate.testing.CrateTestCluster;
import io.crate.testing.CrateTestServer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class CrateClientIntegrationTest extends Assert {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static String tarBallPath;

    static {

        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:crate-*.tar.gz");
        FileVisitor<Path> matcherVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attribs) {
                if (matcher.matches(file.getFileName())) {
                    tarBallPath = file.toAbsolutePath().toString();
                    return FileVisitResult.TERMINATE;
                }
                return FileVisitResult.CONTINUE;
            }
        };
        try {
            Files.walkFileTree(Paths.get(
                    System.getProperty("project_root") + "/app/build/distributions/"), matcherVisitor);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        assert tarBallPath != null;
    }


    @ClassRule
    public static final CrateTestCluster testCluster = CrateTestCluster.fromFile(tarBallPath)
            .workingDir(Paths.get(System.getProperty("project_build_dir"), "crate-testing")).numberOfNodes(1).build();

    protected String serverAddress() {
        CrateTestServer server = testCluster.randomServer();
        return server.crateHost() + ':' + server.transportPort();
    }

    private void waitForZeroCount(CrateClient client, String stmt) {
        for (int i = 1; i < 10; i++) {
            SQLResponse r = client.sql(stmt).actionGet();
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

    protected void ensureYellow(CrateClient client) {
        waitForZeroCount(client, "select count(*) from sys.shards where \"primary\" = true and state <> 'STARTED'");
    }

    protected void ensureGreen(CrateClient client) {
        waitForZeroCount(client, "select count(*) from sys.shards where state <> 'STARTED'");
    }


}
