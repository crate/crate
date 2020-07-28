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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import io.crate.action.sql.SQLActionException;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
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

import static com.carrotsearch.randomizedtesting.RandomizedTest.newTempDir;
import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
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
    public void testCopyFromFileWithJsonExtension() throws Exception {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));

        execute("select quote from quotes where id = 1");
        assertThat(response.rows()[0][0], is("Don't pa\u00f1ic."));
    }

    @Test
    public void testCopyFromFileWithCSVOption() {
        execute("create table quotes (id int primary key, " +
            "quote string index using fulltext) with (number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ? with (format='csv')", new Object[]{copyFilePath + "test_copy_from_csv.ext"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));

        execute("select quote from quotes where id = 1");
        assertThat(response.rows()[0][0], is("Don't pa\u00f1ic."));
    }

    @Test
    public void testCopyFromFileWithUmlautsWhitespacesAndGlobs() throws Exception {
        execute("create table t (id int primary key, name string) clustered into 1 shards with (number_of_replicas = 0)");
        File tmpFolder = folder.newFolder("äwesöme földer");
        File file = new File(tmpFolder, "süpär.json");

        List<String> lines = Collections.singletonList("{\"id\": 1, \"name\": \"Arthur\"}");
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ?", new Object[]{Paths.get(tmpFolder.toURI()).toUri().toString() + "s*.json"});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testCopyFromWithOverwriteDuplicates() throws Exception {
        execute("create table t (id int primary key) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (id) values (?)", new Object[][]{
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{4}
        });
        execute("refresh table t");

        File tmpExport = folder.newFolder("tmpExport");
        String uriTemplate = Paths.get(tmpExport.toURI()).toUri().toString();
        execute("copy t to directory ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(4L));
        execute("copy t from ?", new Object[]{uriTemplate + "*"});
        assertThat(response.rowCount(), is(0L));
        execute("copy t from ? with (overwrite_duplicates = true, shared=true)",
            new Object[]{uriTemplate + "*"});
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

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertEquals(6L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(6L, response.rowCount());
        assertThat(response.rows()[0].length, is(2));
    }

    @Test
    public void testCopyFromFilePattern() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas=0)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePathShared + "*.json"});
        assertEquals(6L, response.rowCount());
        refresh();

        execute("select * from quotes");
        assertEquals(6L, response.rowCount());
    }

    @Test
    public void testCopyFromFileWithEmptyLine() throws Exception {
        execute("create table foo (id integer primary key) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        File newFile = folder.newFile();

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            writer.write("{\"id\":1}\n");
            writer.write("\n");
            writer.write("{\"id\":2}\n");
        }
        execute("copy foo from ?", new Object[]{Paths.get(newFile.toURI()).toUri().toString()});
        assertEquals(2L, response.rowCount());
        refresh();

        execute("select * from foo order by id");
        assertThat(response.rows()[0][0], is(1));
        assertThat(response.rows()[1][0], is(2));
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testCopyFromFileWithInvalidColumns() throws Exception {
        execute("create table foo (id integer primary key) clustered into 1 shards " +
                "with (number_of_replicas=0, column_policy='dynamic')");
        ensureYellow();
        File newFile = folder.newFile();

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            writer.write("{\"id\":1, \"_invalid\":1}\n");
            writer.write("{\"id\":2, \"invalid['index']\":2}\n");
            writer.write("{\"id\":3, \"invalid['_invalid']\":3}\n");
            writer.write("{\"id\":4, \"valid\": {\"_valid\": 4}}\n");
        }

        execute("copy foo from ?", new Object[]{Paths.get(newFile.toURI()).toUri().toString()});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("select * from foo order by id");

        // Check columns.
        assertEquals(2L, response.cols().length);
        assertEquals("valid", response.cols()[1]);

        // Check data of column.
        assertThat(response.rows()[0][0], is(4));
        HashMap data = (HashMap)response.rows()[0][1];
        // The inner value will result in an Long type as we rely on ES mappers here and the dynamic ES parsing
        // will define integers as longs (no concrete type was specified so use long to be safe)
        assertThat(data.get("_valid"), is(4L));
    }

    @Test
    public void testCopyFromInvalidJson() throws Exception {
        execute("create table foo (id integer primary key) clustered into 1 shards with (number_of_replicas=0)");
        ensureYellow();
        File newFile = folder.newFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            writer.write("{|}");
        }
        execute("copy foo from ?", new Object[]{Paths.get(newFile.toURI()).toUri().toString()});
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testCopyFromFileIntoSinglePartition() throws Exception {
        execute("CREATE TABLE quotes (id INTEGER, quote STRING) PARTITIONED BY (id)");
        ensureGreen();
        execute("COPY quotes PARTITION (id = 1) FROM ? WITH (shared = true)", new Object[]{
            copyFilePath + "test_copy_from.json"});
        refresh();

        execute("SELECT * FROM quotes");
        assertEquals(3L, response.rowCount());
    }

    @Test
    public void testCopyFromFileWithCompression() throws Exception {
        execute("create table quotes (id int, " +
                "quote string)");
        ensureGreen();

        execute("copy quotes from ? with (compression='gzip')", new Object[]{copyFilePath + "test_copy_from.gz"});
        refresh();

        execute("select * from quotes");
        assertEquals(6L, response.rowCount());
    }

    @Test
    public void testCopyFromWithGeneratedColumn() throws Exception {
        execute("create table quotes (" +
                " id int," +
                " quote string," +
                " gen_quote as concat(quote, ' This is awesome!')" +
                ")");
        ensureYellow();

        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from.json"});
        refresh();

        execute("select gen_quote from quotes limit 1");
        assertThat((String) response.rows()[0][0], endsWith("This is awesome!"));
    }

    @Test
    public void testCopyFromWithInvalidGivenGeneratedColumn() throws Exception {
        execute("create table quotes (" +
                " id int," +
                " quote as cast(id as string)" +
                ")");
        ensureYellow();

        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response.rowCount(), is(3L));
        refresh();

        // quote is not generated through expression but read from source without validation
        execute("select quote from quotes order by id limit 1");
        assertThat((String) response.rows()[0][0], is("Don't pañic."));
    }

    @Test
    public void testCopyFromToPartitionedTableWithGeneratedColumn() throws Exception {
        execute("create table quotes (" +
                " id int," +
                " quote string," +
                " gen_quote as concat(quote, ' Partitioned by awesomeness!')" +
                ") partitioned by (gen_quote)");

        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from.json"});
        refresh();

        execute("select gen_quote from quotes limit 1");
        assertThat((String) response.rows()[0][0], endsWith("Partitioned by awesomeness!"));
    }

    @Test
    public void testCopyFromToPartitionedTableWithNullValue() {
        execute("CREATE TABLE times (" +
                "   time timestamp with time zone" +
                ") partitioned by (time)");
        ensureYellow();

        execute("copy times from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from_null_value.json"});
        refresh();

        execute("select time from times");
        assertThat(response.rowCount(), is(1L));
        assertNull(response.rows()[0][0]);
    }

    @Test
    public void testCopyFromIntoPartitionWithInvalidGivenGeneratedColumnAsPartitionKey() throws Exception {
        // test that rows are imported into defined partition even that the partition value does not match the
        // generated column expression value
        execute("create table quotes (" +
                " id int," +
                " quote string," +
                " id_str as cast(id+1 as string)" +
                ") partitioned by (id_str)");
        ensureYellow();

        execute("copy quotes partition (id_str = 1) from ? with (shared=true)", new Object[]{
            copyFilePath + "test_copy_from.json"});
        assertThat(response.rowCount(), is(3L));
        refresh();

        execute("select * from quotes where id_str = 1");
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testCopyToFile() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(containsString("Using COPY TO without specifying a DIRECTORY is not supported"));

        execute("create table singleshard (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("copy singleshard to '/tmp/file.json'");
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(7L));
        String[] list = folder.getRoot().list();
        assertThat(list, is(notNullValue()));
        assertThat(list.length, greaterThanOrEqualTo(1));
        for (String file : list) {
            assertThat(file, startsWith("characters_"));
        }

        List<String> lines = new ArrayList<>(7);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path path : stream) {
            lines.addAll(Files.readAllLines(path, StandardCharsets.UTF_8));
        }
        assertThat(lines.size(), is(7));
        for (String line : lines) {
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
    }

    @Test
    public void testCopyToWithCompression() throws Exception {
        execute("create table singleshard (name string) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into singleshard (name) values ('foo')");
        execute("refresh table singleshard");

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy singleshard to DIRECTORY ? with (compression='gzip')", new Object[]{uriTemplate});

        assertThat(response.rowCount(), is(1L));

        String[] list = folder.getRoot().list();
        assertThat(list, is(notNullValue()));
        assertThat(list.length, is(1));
        String file = list[0];
        assertThat(file, both(startsWith("singleshard_")).and(endsWith(".json.gz")));

        long size = Files.size(Paths.get(folder.getRoot().toURI().resolve(file)));
        assertThat(size, is(35L));
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
        for (Path entry : stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        Path path = Paths.get(folder.getRoot().toURI().resolve("characters_0_.json"));
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
    public void testCopyToFileColumnsJsonObjectOutput() throws Exception {
        execute("create table singleshard (name string, test object as (foo string)) clustered into 1 shards with (number_of_replicas = 0)");
        ensureYellow();
        execute("insert into singleshard (name, test) values ('foobar', {foo='bar'})");
        execute("refresh table singleshard");

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy singleshard (name, test['foo']) to DIRECTORY ? with (format='json_object')", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(1L));

        String[] list = folder.getRoot().list();
        assertThat(list, is(notNullValue()));
        assertThat(list.length, is(1));
        List<String> lines = Files.readAllLines(
            Paths.get(folder.getRoot().toURI().resolve(list[0])), StandardCharsets.UTF_8);

        assertThat(lines.size(), is(1));
        for (String line : lines) {
            assertThat(line, startsWith("{"));
            assertThat(line, endsWith("}"));
        }
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
    public void testCopyFromNestedArrayRow() throws Exception {
        // assert that rows with nested arrays aren't imported
        execute("create table users (id int, " +
            "name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("copy users from ? with (shared=true)", new Object[]{
            nestedArrayCopyFilePath + "nested_array_copy_from.json"});
        assertEquals(1L, response.rowCount()); // only 1 document got inserted
        refresh();

        execute("select * from users");
        assertThat(response.rowCount(), is(1L));

        assertThat(printedTable(response.rows()), is("2| Trillian\n"));
    }

    @Test
    public void testCopyToWithGeneratedColumn() {
        execute("CREATE TABLE foo (" +
                "   day TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS date_trunc('day', timestamp)," +
                "   timestamp TIMESTAMP WITH TIME ZONE" +
                ") PARTITIONED BY (day)");
        ensureYellow();
        execute("insert into foo (timestamp) values (1454454000377)");
        refresh();
        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy foo to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testCopyFromWithRoutingInPK() throws Exception {
        execute("create table t (i int primary key, c string primary key, a int)" +
            " clustered by (c) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (i, c) values (1, 'clusteredbyvalue'), (2, 'clusteredbyvalue')");
        refresh();

        String uri = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy t to directory ?", new Object[]{uri});
        assertThat(response.rowCount(), is(2L));

        execute("delete from t");
        refresh();

        execute("copy t from ? with (shared=true)", new Object[]{uri + "t_*"});
        refresh();

        // only one shard should have all imported rows, since we have the same routing for both rows
        response = execute("select count(*) from sys.shards where num_docs>0 and table_name='t'");
        assertThat(response.rows()[0][0], is(1L));
    }

    @Test
    public void testCopyFromTwoHttpUrls() throws Exception {
        execute("create blob table blobs with (number_of_replicas = 0)");
        execute("create table names (id int primary key, name string) with (number_of_replicas = 0)");
        ensureYellow();

        String r1 = "{\"id\": 1, \"name\":\"Marvin\"}";
        String r2 = "{\"id\": 2, \"name\":\"Slartibartfast\"}";
        List<String> urls = List.of(upload("blobs", r1), upload("blobs", r2));

        execute("copy names from ?", new Object[]{urls});
        assertThat(response.rowCount(), is(2L));
        execute("refresh table names");
        execute("select name from names order by id");
        assertThat(printedTable(response.rows()), is("Marvin\nSlartibartfast\n"));
    }

    @Test
    public void testCopyFromTwoUriMixedSchemaAndWildcardUse() throws Exception {
        execute("create blob table blobs with (number_of_replicas = 0)");
        execute("create table names (id int primary key, name string) with (number_of_replicas = 0)");

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        File file = new File(tmpDir.toFile(), "names.json");
        String r1 = "{\"id\": 1, \"name\": \"Arthur\"}";
        String r2 = "{\"id\": 2, \"name\":\"Slartibartfast\"}";

        Files.write(file.toPath(), Collections.singletonList(r1), StandardCharsets.UTF_8);
        List<String> urls = List.of(tmpDir.toUri().toString() + "*.json", upload("blobs", r2));

        execute("copy names from ?", new Object[]{urls});
        assertThat(response.rowCount(), is(2L));
        execute("refresh table names");
        execute("select name from names order by id");
        assertThat(printedTable(response.rows()), is("Arthur\nSlartibartfast\n"));
    }

    @Test
    public void testCopyFromIntoTableWithClusterBy() throws Exception {
        execute("create table quotes (id int, quote string) " +
            "clustered by (id)" +
            "with (number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ? with (shared = true)", new Object[]{copyFilePath + "test_copy_from.json"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select quote from quotes where id = 2");
        assertThat((String) response.rows()[0][0], containsString("lot of time"));
    }

    @Test
    public void testCopyFromIntoTableWithPkAndClusterBy() throws Exception {
        execute("create table quotes (id int primary key, quote string) " +
            "clustered by (id)" +
            "with (number_of_replicas = 0)");
        ensureYellow();

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertEquals(3L, response.rowCount());
        refresh();

        execute("select quote from quotes where id = 3");
        assertThat((String) response.rows()[0][0], containsString("Time is an illusion."));
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

    private static Path tmpFileWithLines(Iterable<String> lines) throws IOException {
        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));
        tmpFileWithLines(lines, "data.json", target);
        return Files.createSymbolicLink(tmpDir.resolve("link"), target);
    }

    private static void tmpFileWithLines(Iterable<String> lines, String filename, Path target) throws IOException {
        File file = new File(target.toFile(), filename);
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
    }

    @Test
    public void testCopyFromSymlinkFolderWithWildcard() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "*"
        });
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testCopyFromSymlinkFolderWithPrefixedWildcard() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "d*"
        });
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testCopyFromSymlinkFolderWithSuffixedWildcard() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "*.json"
        });
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testCopyFromFileInSymlinkFolder() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "data.json"
        });
        assertThat(response.rowCount(), is(3L));
    }

    @Test
    public void testCopyWithGeneratedPartitionColumnThatIsPartOfPrimaryKey() throws Exception {
        execute(
            "create table t1 (" +
            "   guid string," +
            "   ts timestamp with time zone," +
            "   g_ts_month timestamp with time zone generated always as date_trunc('month', ts)," +
            "   primary key (guid, g_ts_month)" +
            ") partitioned by (g_ts_month)");
        ensureYellow();

        Path path = tmpFileWithLines(Arrays.asList(
            "{\"guid\": \"a\", \"ts\": 1496275200000}",
            "{\"guid\": \"b\", \"ts\": 1496275300000}"
        ));
        execute("copy t1 from ? with (shared=true)", new Object[] { path.toUri().toString() + "*.json"});
        assertThat(response.rowCount(), is(2L));

        execute("copy t1 partition (g_ts_month = 1496275200000) from ? with (shared=true, overwrite_duplicates=true)",
            new Object[] { path.toUri().toString() + "*.json"});
        assertThat(response.rowCount(), is(2L));
    }

    @Test
    public void testCopyFromReturnSummaryWithFailedRows() throws Exception {
        execute("create table t1 (id int primary key, ts timestamp with time zone)");

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        Path target = Files.createDirectories(tmpDir.resolve("target"));

        tmpFileWithLines(Arrays.asList(
            "{\"id\": 1, \"ts\": 1496275200000}",
            "{\"id\": 2, \"ts\": 1496275300000}"
        ), "data1.json", target);
        tmpFileWithLines(Arrays.asList(
            "{\"id\": 2, \"ts\": 1496275200000}",       // <-- duplicate key
            "{\"id\": 3, \"ts\": 1496275300000}"
        ), "data2.json", target);
        tmpFileWithLines(Arrays.asList(
            "{\"id\": 4, \"ts\": 1496275200000}",
            "{\"id\": 5, \"ts\": \"May\"}",              // <-- invalid timestamp
            "{\"id\": 7, \"ts\": \"Juli\"}"              // <-- invalid timestamp
        ), "data3.json", target);
        tmpFileWithLines(Arrays.asList(
            "foo",                                      // <-- invalid json
            "{\"id\": 6, \"ts\": 1496275200000}"
        ), "data4.json", target);

        execute("copy t1 from ? with (shared=true) return summary", new Object[]{target.toUri().toString() + "*"});
        String result = printedTable(response.rows());

        // one of the first files should be processed without any error
        assertThat(result, containsString("| 2| 0| {}"));
        // one of the first files will have a duplicate key error
        assertThat(result, containsString("| 1| 1| {A document with the same primary key exists already={count=1, line_numbers=["));
        // file `data3.json` has a invalid timestamp error
        assertThat(result, containsString("data3.json| 1| 2| {failed to parse field [ts] of type [date]={count=2, line_numbers=["));
        // file `data4.json` has an invalid json item entry
        assertThat(result, containsString("data4.json| 1| 1| {JSON parser error: "));
    }

    @Test
    public void testCopyFromReturnSummaryWithFailedURI() throws Exception {
        execute("create table t1 (id int primary key, ts timestamp with time zone)");

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        String tmpDirStr = tmpDir.toUri().toString();

        String filename = "nonexistingfile.json";
        execute("copy t1 from ? return summary", new Object[]{tmpDirStr + filename});
        assertThat(response.rowCount(), is((long) internalCluster().numDataNodes()));

        boolean isRunningOnWindows = System.getProperty("os.name").startsWith("Windows");
        String expected = "(No such file or directory)";
        if (isRunningOnWindows) {
            expected = "(The system cannot find the file specified)";
        }
        for (Object[] row : response.rows()) {
            assertThat((String) row[1], endsWith(filename));
            assertThat(row[2], nullValue());
            assertThat(row[3], nullValue());
            assertThat(((Map<String, Object>) row[4]).keySet(), contains(containsString(expected)));
        }

        // with shared=true, only 1 data node must try to process the uri
        execute("copy t1 from ? with (shared=true) return summary", new Object[]{tmpDirStr + filename});
        assertThat(response.rowCount(), is(1L));

        for (Object[] row : response.rows()) {
            assertThat((String) row[1], endsWith(filename));
            assertThat(row[2], nullValue());
            assertThat(row[3], nullValue());
            assertThat(((Map<String, Object>) row[4]).keySet(), contains(containsString(expected)));
        }

        // with shared=true and wildcards all nodes will try to match a file
        filename =  "*.json";
        execute("copy t1 from ? with (shared=true) return summary", new Object[]{tmpDirStr + filename});
        assertThat(response.rowCount(), is((long) internalCluster().numDataNodes()));

        for (Object[] row : response.rows()) {
            assertThat((String) row[1], endsWith("*.json"));
            assertThat(row[2], nullValue());
            assertThat(row[3], nullValue());
            assertThat(((Map<String, Object>) row[4]).keySet(), contains(containsString("Cannot find any URI matching:")));
        }
    }
}
