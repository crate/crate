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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.HashMap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.Asserts;

public class RepositoryIntegrationTest extends IntegTestCase {

    @ClassRule
    public static TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMPORARY_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Test
    public void test_create_repo_in_invalid_path() throws Exception {
        String stmt = "CREATE REPOSITORY repo1 TYPE \"fs\" WITH (location = '/invalid/location')";
        assertThatThrownBy(() -> execute(stmt)).hasMessageContaining(
            "[repo1] location [/invalid/location] doesn't match any of the locations specified by path.repo");
    }

    @Test
    public void testDropExistingRepository() throws Exception {
        execute("CREATE REPOSITORY existing_repo TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{
                TEMPORARY_FOLDER.newFolder().getAbsolutePath()
            });
        waitNoPendingTasksOnAll();
        execute("DROP REPOSITORY existing_repo");
        assertThat(response.rowCount()).isEqualTo(1L);

        waitNoPendingTasksOnAll();
        execute("select * from sys.repositories where name = ? ", new Object[]{"existing_repo"});
        assertThat(response.rowCount()).isEqualTo(0L);
    }

    @Test
    public void testCreateRepository() throws Throwable {
        String repoLocation = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        execute("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{
                repoLocation
            });
        waitNoPendingTasksOnAll();
        execute("select * from sys.repositories where name ='myRepo'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((String) response.rows()[0][0]).isEqualTo("myRepo");
        HashMap<String, String> settings = (HashMap) response.rows()[0][1];
        assertThat(settings.get("compress")).isEqualTo("true");
        assertThat(new File(settings.get("location")).getAbsolutePath()).isEqualTo(repoLocation);
        assertThat((String) response.rows()[0][2]).isEqualTo("fs");
    }

    @Test
    public void testCreateExistingRepository() throws Throwable {
        String repoLocation = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        execute("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location=?, compress=True)",
            new Object[]{
                repoLocation
            });
        waitNoPendingTasksOnAll();
        Asserts.assertSQLError(() -> execute("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location=?, compress=True)",
                                   new Object[]{repoLocation}))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(CONFLICT, 4095)
            .hasMessageContaining("Repository 'myRepo' already exists");

    }
}
