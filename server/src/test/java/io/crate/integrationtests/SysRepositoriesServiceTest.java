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

import static io.crate.rest.action.HttpErrorStatus.REPOSITORY_UNKNOWN;
import static io.crate.rest.action.HttpErrorStatus.REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY;
import static io.crate.rest.action.HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
import static io.crate.testing.Asserts.assertSQLError;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.crate.testing.UseJdbc;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StringType;

@IntegTestCase.ClusterScope()
public class SysRepositoriesServiceTest extends IntegTestCase {

    @ClassRule
    public static TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("path.repo", TEMP_FOLDER.getRoot().getAbsolutePath())
            .build();
    }

    @Before
    public void setUpRepositories() throws Exception {
        String location = new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath();
        execute(
            "CREATE REPOSITORY \"test-repo\" TYPE fs WITH (location = ?, chunk_size = '5k', compress = false)",
            new Object[]{location}
        );
    }

    @Test
    @UseJdbc(0) // missing column types
    public void testQueryAllColumns() {
        execute("select * from sys.repositories");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.cols().length).isEqualTo(3);
        assertThat(response.cols()).isEqualTo(new String[]{"name", "settings", "type"});
        assertThat(response.columnTypes()).isEqualTo(new DataType[]{StringType.INSTANCE, DataTypes.UNTYPED_OBJECT, StringType.INSTANCE});
        assertThat((String) response.rows()[0][0]).isEqualTo("test-repo");

        Map<String, Object> settings = (Map<String, Object>) response.rows()[0][1];
        assertThat(settings).hasSize(3);
        assertThat((String) settings.get("location")).isEqualTo(new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath());
        assertThat((String) settings.get("chunk_size")).isEqualTo("5k");
        assertThat((String) settings.get("compress")).isEqualTo("false");

        assertThat((String) response.rows()[0][2]).isEqualTo("fs");
    }

    @Test
    public void test_create_missing_required_properties() {
        assertSQLError(() -> execute("CREATE REPOSITORY \"test-repo-1\" TYPE fs WITH (compress = false)"))
            .hasHTTPError(STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX.httpResponseStatus(), STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX.errorCode())
            .hasMessageContaining("The following required parameters are missing to create a repository of type \"fs\": [location]");
    }

    @Test
    public void test_create_repository_already_exists() {
        assertSQLError(() -> execute("CREATE REPOSITORY \"test-repo\" TYPE fs WITH (location = 'whatever')"))
            .hasHTTPError(REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY.httpResponseStatus(), REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY.errorCode())
            .hasMessageContaining("Repository 'test-repo' already exists");
    }

    // ALTER REPOSITORY tests, happy path
    @Test
    public void test_alter_repository_set_and_revert_setting() {
        execute("alter repository \"test-repo\" set (compress = true)");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertRepositorySettings("test-repo", Map.of(
            "compress", "true",
            "chunk_size", "5k",
            "location", new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath()
        ));

        execute("alter repository \"test-repo\" set (compress = false)");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertRepositorySettings("test-repo", Map.of(
            "compress", "false",
            "chunk_size", "5k",
            "location", new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath()
        ));

        // test setting multiple properties in one query
        execute("alter repository \"test-repo\" set (compress = true, chunk_size = '10k')");
        assertThat(response.rowCount()).isEqualTo(1L);

        assertRepositorySettings("test-repo", Map.of(
            "compress", "true",
            "chunk_size", "10k",
            "location", new File(TEMP_FOLDER.getRoot(), "backup").getAbsolutePath()
        ));
    }

    // ALTER REPOSITORY tests, error handling
    @Test
    public void test_alter_repository_non_existent_repository() {
        assertSQLError(() -> execute("alter repository \"does_not_exist\" set (compress = true)"))
            .hasHTTPError(REPOSITORY_UNKNOWN.httpResponseStatus(), REPOSITORY_UNKNOWN.errorCode())
            .hasMessageContaining("[does_not_exist] missing");

        // testing the path where no repositories exist at all
        execute("drop repository \"test-repo\"");
        assertThat(response.rowCount()).isEqualTo(1);

        assertSQLError(() -> execute("alter repository \"test-repo\" set (compress = true)"))
            .hasHTTPError(REPOSITORY_UNKNOWN.httpResponseStatus(), REPOSITORY_UNKNOWN.errorCode())
            .hasMessageContaining("[test-repo] missing");
    }

    @Test
    public void test_alter_repository_invalid_setting_name() {
        assertSQLError(() -> execute("alter repository \"test-repo\" set (invalid_setting = true)"))
            .hasHTTPError(STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX.httpResponseStatus(), STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX.errorCode())
            .hasMessageContaining("Setting 'invalid_setting' is not supported");
    }

    private void assertRepositorySettings(String repoName, Map<String, Object> expectedSettings) {
        execute("select settings from sys.repositories where name = ?", new Object[]{repoName});
        assertThat(response.rowCount()).isEqualTo(1L);
        Map<String, Object> settings = (Map<String, Object>) response.rows()[0][0];
        assertThat(settings).containsAllEntriesOf(expectedSettings);
    }
}
