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

import static com.carrotsearch.randomizedtesting.RandomizedTest.newTempDir;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import com.carrotsearch.randomizedtesting.LifecycleScope;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class CopyIntegrationTest extends SQLHttpIntegrationTest {

    private final String copyFilePath =
        Paths.get(getClass().getResource("/essetup/data/copy").toURI()).toUri().toString();
    private final String copyFilePathShared =
        Paths.get(getClass().getResource("/essetup/data/copy/shared").toURI()).toUri().toString();
    private final String nestedArrayCopyFilePath =
        Paths.get(getClass().getResource("/essetup/data/nested_array").toURI()).toUri().toString();

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public CopyIntegrationTest() throws URISyntaxException {
    }

    @Test
    public void testCopyFromUnknownDirectory() {
        execute("create table t (a int)");
        ensureYellow();
        execute("copy t from 'file:///tmp/unknown_dir/*'");
    }

    @Test
    public void testCopyFromFileWithCSVOptionWithNoHeader() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0)");

        execute("copy quotes from ? with (format='csv', header=false)", new Object[]{copyFilePath + "test_copy_from_csv_no_header.ext"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));

        execute("select quote from quotes where id = 1");
        assertThat(response.rows()[0][0], is("Don't pa\u00f1ic."));
    }

    @Test
    public void testCopyToWithWhere() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters where gender = 'female' to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testCopyToWithWhereOnPrimaryKey() throws Exception {
        execute("create table t1 (id int primary key) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1)");
        execute("refresh table t1");

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy t1 where id = 1 to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testCopyToWithWhereNoMatch() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters where gender = 'foo' to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(0L));
    }



    @Test
    public void testCopyToWithGeneratedColumn() {
        execute("CREATE TABLE foo (" +
                "   day TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', timestamp)," +
                "   timestamp TIMESTAMP WITH TIME ZONE" +
                ") PARTITIONED BY (day)");
        execute("insert into foo (timestamp) values (1454454000377)");
        refresh();
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy foo to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(1L));
    }




    private Path setUpTableAndSymlink(String tableName) throws IOException {
        execute(String.format(Locale.ENGLISH,
            "create table %s (a int) with (number_of_replicas = 0)",
            tableName));

        String r1 = "{\"a\": 1}";
        String r2 = "{\"a\": 2}";
        String r3 = "{\"a\": 3}";
        return tmpFileWithLines(List.of(r1, r2, r3));
    }

    public static Path tmpFileWithLines(Iterable<String> lines) throws IOException {
        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(lines, "data.json", target);
        return Files.createSymbolicLink(tmpDir.resolve("link"), target);
    }

    public static void tmpFileWithLines(Iterable<String> lines, String filename, Path target) throws IOException {
        File file = new File(target.toFile(), filename);
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
    }




    @Test
    public void test_copy_from_csv_file_with_empty_string_as_null_property() throws Exception {
        execute(
            "CREATE TABLE t (id int primary key, name text) " +
            "CLUSTERED INTO 1 SHARDS ");
        File file = folder.newFile(UUID.randomUUID().toString());

        List<String> lines = List.of(
           // "id,name", // new change is integrated only to the no-header case
            "1, \"foo\"",
            "2,\"\"",
            "3,"
        );
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute(
            "COPY t FROM ? WITH (format='csv', header = false, empty_string_as_null=true)",
            new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(response.rowCount(), is(3L));
        refresh();

        execute("SELECT * FROM t ORDER BY id");
        assertThat(
            printedTable(response.rows()),
            is("1| foo\n2| NULL\n3| NULL\n"));
    }


}
