/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.integrationtests;

import com.google.common.base.Joiner;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLResponse;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.testing.TestingHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class CopyIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private String copyFilePath = getClass().getResource("/essetup/data/copy").getPath();
    private String nestedArrayCopyFilePath = getClass().getResource("/essetup/data/nested_array").getPath();

    private Setup setup = new Setup(sqlExecutor);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Test
    public void testCopyFromFile() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0)");
        ensureYellow();

        String uriPath = Joiner.on("/").join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{uriPath});
        assertEquals(3L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));

        execute("select quote from quotes where id = 1");
        assertThat((String) response.rows()[0][0], is("Don't pa\u00f1ic."));
    }

    @Test
    public void testCopyFromFileWithUmlautsWhitespacesAndGlobs() throws Exception {
        execute("create table t (id int primary key, name string) clustered into 1 shards with (number_of_replicas = 0)");
        File tmpFolder = folder.newFolder("äwesöme földer");
        File file = new File(tmpFolder, "süpär.json");

        List<String> lines = Arrays.asList("{\"id\": 1, \"name\": \"Arthur\"}");
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ?", new Object[] {tmpFolder.getAbsolutePath() + "/s*.json"});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testCopyFromWithOverwriteDuplicates() throws Exception {
        execute("create table t (id int primary key) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (id) values (?)", new Object[][] {
                new Object[] { 1 },
                new Object[] { 2 },
                new Object[] { 3 },
                new Object[] { 4 }
        });
        execute("refresh table t");

        File tmpExport = folder.newFolder("tmpExport");
        execute("copy t to directory ?", new Object[] { tmpExport.getAbsolutePath()});
        assertThat(response.rowCount(), is(4L));
        execute("copy t from ?", new Object[]{String.format("%s/*", tmpExport.getAbsolutePath())});
        assertThat(response.rowCount(), is(0L));
        execute("copy t from ? with (overwrite_duplicates = true, shared=true)",
                new Object[]{String.format("%s/*", tmpExport.getAbsolutePath())});
        assertThat(response.rowCount(), is(4L));
        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(((Long) response.rows()[0][0]), is(4L));
    }

    @Test
    public void testCopyFromFileWithoutPK() throws Exception {
        execute("create table quotes (id int, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureYellow();

        String uriPath = Joiner.on("/").join(copyFilePath, "test_copy_from.json");
        execute("copy quotes from ?", new Object[]{uriPath});
        assertEquals(6L, response.rowCount());
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        refresh();

        execute("select * from quotes");
        assertEquals(6L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));
    }

    @Test
    public void testCopyFromDirectory() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureYellow();

        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "/*"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyFromFilePattern() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureYellow();

        String uriPath = Joiner.on("/").join(copyFilePath, "*.json");
        execute("copy quotes from ?", new Object[]{uriPath});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyFromFileWithPartition() throws Exception {
        execute("create table quotes (id int, " +
                "quote string) partitioned by (id)");
        ensureGreen();
        String filePath = Joiner.on(File.separator).join(copyFilePath, "test_copy_from.json");
        execute("copy quotes partition (id = 1) from ? with (shared=true)", new Object[]{filePath});
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyToFile() throws Exception {
        execute("create table singleshard (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into singleshard (name) values ('foo')");
        execute("refresh table singleshard");

        String uri = Paths.get(folder.getRoot().toURI()).resolve("testsingleshard.json").toUri().toString();
        SQLResponse response = execute("copy singleshard to ?", new Object[] { uri });
        assertThat(response.rowCount(), is(1L));
        List<String> lines = Files.readAllLines(
                Paths.get(folder.getRoot().toURI().resolve("testsingleshard.json")), UTF8);

        assertThat(lines.size(), is(1));
        for (String line : lines) {
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testCopyColumnsToDirectory() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters (name, details['job']) to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.cols().length, is(0));
        assertThat(response.rowCount(), is(7L));
        List<String> lines = new ArrayList<>(7);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry: stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        Path path = Paths.get(folder.getRoot().toURI().resolve("characters_1_.json"));
        assertTrue(path.toFile().exists());
        assertThat(lines.size(), is(7));

        boolean foundJob = false;
        boolean foundName = false;
        for (String line : lines) {
            foundName = foundName || line.contains("Arthur Dent");
            foundJob = foundJob || line.contains("Sandwitch Maker");
            assertThat(line.split(",").length, is(2));
            assertThat(line.trim(), startsWith("["));
            assertThat(line.trim(), endsWith("]"));
        }
        assertTrue(foundJob);
        assertTrue(foundName);
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(7L));
        List<String> lines = new ArrayList<>(7);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry: stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        Path path = Paths.get(folder.getRoot().toURI().resolve("characters_1_.json"));
        assertTrue(path.toFile().exists());

        assertThat(lines.size(), is(7));
        for (String line : lines) {
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testCopyFromNestedArrayRow() throws Exception {
        // assert that rows with nested arrays aren't imported
        execute("create table users (id int, " +
                "name string) with (number_of_replicas=0)");
        ensureYellow();
        String uriPath = Joiner.on("/").join(nestedArrayCopyFilePath, "nested_array_copy_from.json");
        execute("copy users from ? with (shared=true)", new Object[]{uriPath});
        assertEquals(1L, response.rowCount()); // only 1 document got inserted
        refresh();

        execute("select * from users");
        assertThat(response.rowCount(), is(1L));

        assertThat(TestingHelpers.printedTable(response.rows()), is("2| Trillian\n"));
    }

    @Test
    public void testCopyToDirectoryPath() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(startsWith("Failed to open output: 'Output path is a directory: "));
        execute("create table characters (" +
                " race string," +
                " gender string," +
                " age long," +
                " birthdate timestamp," +
                " name string," +
                " details object" +
                ") with (number_of_replicas=0)");
        ensureYellow();

        String directoryUri = Paths.get(folder.newFolder().toURI()).toUri().toString();
        execute("COPY characters TO ?", new Object[]{directoryUri});
    }

    @Test
    public void testCopyFromWithRoutingInPK() throws Exception {
        execute("create table t (i int primary key, c string primary key, a int)" +
                " clustered by (c) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, c) values (1, 'clusteredbyvalue'), (2, 'clusteredbyvalue')");
        refresh();

        String uri = Paths.get(folder.getRoot().toURI()).toString();
        SQLResponse response = execute("copy t to directory ? with(shared=true)", new Object[] { uri });
        assertThat(response.rowCount(), is(2L));

        execute("delete from t");
        refresh();

        execute("copy t from ? with (shared=true)", new Object[] { uri + "/t_*" });
        refresh();

        // only one shard should have all imported rows, since we have the same routing for both rows
        response = execute("select count(*) from sys.shards where num_docs>0 and table_name='t'");
        assertThat((long) response.rows()[0][0], is(1L));

    }
}
