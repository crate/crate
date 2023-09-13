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
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;

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

import org.elasticsearch.test.IntegTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.carrotsearch.randomizedtesting.LifecycleScope;

import io.crate.testing.Asserts;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseNewCluster;

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
    public void testCopyFromFileWithJsonExtension() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0)");

        execute("copy quotes from ?", new Object[] {copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(3L);
        assertThat(response.rows()[0]).hasSize(2);

        execute("select quote from quotes where id = 1");
        assertThat(response.rows()[0][0]).isEqualTo("Don't pa\u00f1ic.");
    }

    @Test
    public void testCopyFromFileWithCSVOption() {
        execute("create table quotes (id int primary key, " +
            "quote string index using fulltext) with (number_of_replicas = 0)");

        execute("copy quotes from ? with (format='csv')", new Object[]{copyFilePath + "test_copy_from_csv.ext"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(3L);
        assertThat(response.rows()[0]).hasSize(2);

        execute("select quote from quotes where id = 1");
        assertThat(response.rows()[0][0]).isEqualTo("Don't pa\u00f1ic.");
    }

    @Test
    public void testCopyFromFileWithCSVOptionWithDynamicColumnCreation() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0, column_policy = 'dynamic')");

        execute("copy quotes from ? with (format='csv')", new Object[]{copyFilePath + "test_copy_from_csv_extra_column.ext"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(3L);
        assertThat(response.rows()[0]).hasSize(3);

        execute("select quote, comment from quotes where id = 1");
        assertThat(response.rows()[0][0]).isEqualTo("Don't pa\u00f1ic.");
        assertThat(response.rows()[0][1]).isEqualTo("good one");
    }

    @Test
    public void testCopyFromFileWithCSVOptionWithTargetColumns() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext, comment text) with (number_of_replicas = 0)");

        execute("copy quotes(id, quote, comment) from ? with (format='csv')", new Object[]{copyFilePath + "test_copy_from_csv_extra_column.ext"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(3L);
        assertThat(response.rows()[0]).hasSize(3);

        execute("select quote, comment from quotes where id = 1");
        assertThat(response.rows()[0][0]).isEqualTo("Don't pa\u00f1ic.");
        assertThat(response.rows()[0][1]).isEqualTo("good one");
    }

    @Test
    public void testCopyFromFileWithCSVOptionWithNoHeader() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas = 0)");

        execute("copy quotes from ? with (format='csv', header=false)", new Object[]{copyFilePath + "test_copy_from_csv_no_header.ext"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(3L);
        assertThat(response.rows()[0]).hasSize(2);

        execute("select quote from quotes where id = 1");
        assertThat(response.rows()[0][0]).isEqualTo("Don't pa\u00f1ic.");
    }

    @Test
    public void testCopyFromFileWithUmlautsWhitespacesAndGlobs() throws Exception {
        execute("create table t (id int primary key, name string) clustered into 1 shards with (number_of_replicas = 0)");
        File tmpFolder = folder.newFolder("äwesöme földer");
        File file = new File(tmpFolder, "süpär.json");

        List<String> lines = Collections.singletonList("{\"id\": 1, \"name\": \"Arthur\"}");
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ?", new Object[]{Paths.get(tmpFolder.toURI()).toUri().toString() + "s*.json"});
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testCopyFromWithOverwriteDuplicates() throws Exception {
        execute("create table t (id int primary key) with (number_of_replicas = 0)");

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
        assertThat(response).hasRowCount(4L);
        execute("copy t from ?", new Object[]{uriTemplate + "*"});
        assertThat(response).hasRowCount(0L);
        execute("copy t from ? with (overwrite_duplicates = true, shared=true)",
            new Object[]{uriTemplate + "*"});
        assertThat(response).hasRowCount(4L);
        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(((Long) response.rows()[0][0])).isEqualTo(4L);
    }

    @Test
    public void testCopyFromFileWithoutPK() throws Exception {
        execute("create table quotes (id int, " +
                "quote string index using fulltext) with (number_of_replicas=0)");

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(6L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(6L);
        assertThat(response.rows()[0]).hasSize(2);
    }

    @Test
    public void testCopyFromFilePattern() {
        execute("create table quotes (id int primary key, " +
                "quote string index using fulltext) with (number_of_replicas=0)");

        execute("copy quotes from ?", new Object[]{copyFilePathShared + "*.json"});
        assertThat(response).hasRowCount(6L);
        refresh();

        execute("select * from quotes");
        assertThat(response).hasRowCount(6L);
    }

    @Test
    public void testCopyFromFileWithEmptyLine() throws Exception {
        execute("create table foo (id integer primary key) clustered into 1 shards with (number_of_replicas=0)");
        File newFile = folder.newFile();

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            writer.write("{\"id\":1}\n");
            writer.write("\n");
            writer.write("{\"id\":2}\n");
        }
        execute("copy foo from ?", new Object[]{Paths.get(newFile.toURI()).toUri().toString()});
        assertThat(response).hasRowCount(2L);
        refresh();

        execute("select * from foo order by id");
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[1][0]).isEqualTo(2);
    }

    /**
     * Disable JDBC/PSQL as object values are streamed via JSON on the PSQL wire protocol which is not type safe.
     */
    @UseJdbc(0)
    @Test
    public void testCopyFromFileWithInvalidColumns() throws Exception {
        execute("create table foo (id integer primary key) clustered into 1 shards " +
                "with (number_of_replicas=0, column_policy='dynamic')");
        File newFile = folder.newFile();

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            writer.write("{\"id\":1, \"_invalid\":1}\n");
            writer.write("{\"id\":2, \"invalid['index']\":2}\n");
            writer.write("{\"id\":3, \"invalid['_invalid']\":3}\n");
            writer.write("{\"id\":4, \"valid\": {\"_valid\": 4}}\n");
        }

        execute("copy foo from ?", new Object[]{Paths.get(newFile.toURI()).toUri().toString()});
        assertThat(response).hasRowCount(1L);
        refresh();

        execute("select * from foo order by id");

        // Check columns.
        assertThat(response.cols()).hasSize(2);
        assertThat(response.cols()[1]).isEqualTo("valid");

        // Check data of column.
        assertThat(response.rows()[0][0]).isEqualTo(4);
        HashMap<?, ?> data = (HashMap<?, ?>)response.rows()[0][1];
        // The inner value will result in an Long type as we rely on ES mappers here and the dynamic ES parsing
        // will define integers as longs (no concrete type was specified so use long to be safe)
        assertThat(data.get("_valid")).isEqualTo(4L);
    }

    @Test
    public void testCopyFromInvalidJson() throws Exception {
        execute("create table foo (id integer primary key) clustered into 1 shards with (number_of_replicas=0)");
        File newFile = folder.newFile();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8)) {
            writer.write("{|}");
        }
        execute("copy foo from ?", new Object[]{Paths.get(newFile.toURI()).toUri().toString()});
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testCopyFromFileIntoSinglePartition() throws Exception {
        execute("CREATE TABLE quotes (id INTEGER, quote STRING) PARTITIONED BY (id)");
        ensureGreen();
        execute("COPY quotes PARTITION (id = 1) FROM ? WITH (shared = true)", new Object[]{
            copyFilePath + "test_copy_from.json"});
        refresh();

        execute("SELECT * FROM quotes");
        assertThat(response).hasRowCount(3L);
    }

    @Test
    public void testCopyFromFileWithCompression() throws Exception {
        execute("create table quotes (id int, " +
                "quote string)");
        execute("copy quotes from ? with (compression='gzip')", new Object[]{copyFilePath + "test_copy_from.gz"});
        execute("refresh table quotes");
        execute("select * from quotes order by quote");
        assertThat(response).hasRows(
            "1| Don't pañic.",
            "1| Don't pañic.",
            "3| Time is an illusion. Lunchtime doubly so.",
            "3| Time is an illusion. Lunchtime doubly so.",
            "2| Would it save you a lot of time if I just gave up and went mad now?",
            "2| Would it save you a lot of time if I just gave up and went mad now?"
        );
    }

    @Test
    public void testCopyFromWithGeneratedColumn() throws Exception {
        execute("create table quotes (" +
                " id int," +
                " quote string," +
                " gen_quote as concat(quote, ' This is awesome!')" +
                ")");

        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from.json"});
        refresh();

        execute("select gen_quote from quotes limit 1");
        assertThat((String) response.rows()[0][0]).endsWith("This is awesome!");
    }

    @Test
    public void testCopyFromWithInvalidGivenGeneratedColumn() throws Exception {
        execute("create table quotes (" +
                " id int," +
                " quote as cast(id as string)" +
                ")");
        execute("copy quotes from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(0L);
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
        assertThat((String) response.rows()[0][0]).endsWith("Partitioned by awesomeness!");
    }

    @Test
    public void testCopyFromToPartitionedTableWithNullValue() {
        execute("CREATE TABLE times (" +
                "   time timestamp with time zone" +
                ") partitioned by (time)");

        execute("copy times from ? with (shared=true)", new Object[]{copyFilePath + "test_copy_from_null_value.json"});
        refresh();

        execute("select time from times");
        assertThat(response).hasRowCount(1L);
        assertThat(response.rows()[0][0]).isNull();
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

        execute("copy quotes partition (id_str = 1) from ? with (shared=true)", new Object[]{
            copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select * from quotes where id_str = 1");
        assertThat(response).hasRowCount(3L);
    }

    @Test
    public void testCopyToFile() throws Exception {
        execute("create table singleshard (name string) clustered into 1 shards with (number_of_replicas = 0)");

        Asserts.assertSQLError(() -> execute("copy singleshard to '/tmp/file.json'"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Using COPY TO without specifying a DIRECTORY is not supported");
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response).hasRowCount(7L);
        String[] list = folder.getRoot().list();
        assertThat(list).hasSizeGreaterThanOrEqualTo(1);
        for (String file : list) {
            assertThat(file).startsWith("characters_");
        }

        List<String> lines = new ArrayList<>(7);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path path : stream) {
            lines.addAll(Files.readAllLines(path, StandardCharsets.UTF_8));
        }
        assertThat(lines).hasSize(7);
        for (String line : lines) {
            assertThat(line).startsWith("{");
            assertThat(line).endsWith("}");
        }
    }

    @Test
    public void testCopyToWithCompression() throws Exception {
        execute("create table singleshard (name string) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into singleshard (name) values ('foo')");
        execute("refresh table singleshard");

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy singleshard to DIRECTORY ? with (compression='gzip')", new Object[]{uriTemplate});

        assertThat(response).hasRowCount(1L);

        String[] list = folder.getRoot().list();
        assertThat(list).hasSize(1);
        String file = list[0];
        assertThat(file)
            .startsWith("singleshard_")
            .endsWith(".json.gz");

        long size = Files.size(Paths.get(folder.getRoot().toURI().resolve(file)));
        assertThat(size).isEqualTo(35L);
    }

    @Test
    public void testCopyColumnsToDirectory() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters (name, details['job']) to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response.cols()).isEmpty();
        assertThat(response).hasRowCount(7L);
        List<String> lines = new ArrayList<>(7);
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(folder.getRoot().toURI()), "*.json");
        for (Path entry : stream) {
            lines.addAll(Files.readAllLines(entry, StandardCharsets.UTF_8));
        }
        Path path = Paths.get(folder.getRoot().toURI().resolve("characters_0_.json"));
        assertThat(path).exists();
        assertThat(lines).hasSize(7);
        assertThat(lines)
            .anySatisfy(x -> assertThat(x).contains("Sandwitch Maker"))
            .anySatisfy(x -> assertThat(x).contains("Arthur Dent"));
        assertThat(lines).allSatisfy(
            x -> assertThat(x.trim()).startsWith("[").endsWith("]"));
    }

    @Test
    public void testCopyToFileColumnsJsonObjectOutput() throws Exception {
        execute("create table singleshard (name string, test object as (foo string)) clustered into 1 shards with (number_of_replicas = 0)");
        execute("insert into singleshard (name, test) values ('foobar', {foo='bar'})");
        execute("refresh table singleshard");

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy singleshard (name, test['foo']) to DIRECTORY ? with (format='json_object')", new Object[]{uriTemplate});
        assertThat(response).hasRowCount(1L);

        String[] list = folder.getRoot().list();
        assertThat(list).hasSize(1);
        List<String> lines = Files.readAllLines(
            Paths.get(folder.getRoot().toURI().resolve(list[0])), StandardCharsets.UTF_8);

        assertThat(lines).hasSize(1);
        for (String line : lines) {
            assertThat(line).startsWith("{");
            assertThat(line).endsWith("}");
        }
    }

    @Test
    public void testCopyToWithWhere() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters where gender = 'female' to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response).hasRowCount(2L);
    }

    @Test
    public void testCopyToWithWhereOnPrimaryKey() throws Exception {
        execute("create table t1 (id int primary key) with (number_of_replicas = 0)");
        execute("insert into t1 (id) values (1)");
        execute("refresh table t1");

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy t1 where id = 1 to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response).hasRowCount(1L);
    }

    @Test
    public void testCopyToWithWhereNoMatch() throws Exception {
        this.setup.groupBySetup();

        String uriTemplate = Paths.get(folder.getRoot().toURI()).toUri().toString();
        SQLResponse response = execute("copy characters where gender = 'foo' to DIRECTORY ?", new Object[]{uriTemplate});
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void testCopyFromNestedArrayRow() throws Exception {
        // assert that rows with nested arrays aren't imported
        execute("create table users (id int, " +
            "name string) with (number_of_replicas=0, column_policy = 'dynamic')");
        execute("copy users from ? with (shared=true)", new Object[]{
            nestedArrayCopyFilePath + "nested_array_copy_from.json"});
        assertThat(response).hasRowCount(1L); // only 1 document got inserted
        refresh();

        execute("select * from users");
        assertThat(response).hasRowCount(1L);

        assertThat(printedTable(response.rows())).isEqualTo("2| Trillian\n");
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
        assertThat(response).hasRowCount(1L);
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
        assertThat(response).hasRowCount(2L);

        execute("delete from t");
        refresh();

        execute("copy t from ? with (shared=true)", new Object[]{uri + "t_*"});
        refresh();

        // only one shard should have all imported rows, since we have the same routing for both rows
        response = execute("select count(*) from sys.shards where num_docs>0 and table_name='t'");
        assertThat(response.rows()[0][0]).isEqualTo(1L);
    }

    @Test
    public void testCopyFromTwoHttpUrls() throws Exception {
        execute("create blob table blobs with (number_of_replicas = 0)");
        execute("create table names (id int primary key, name string) with (number_of_replicas = 0)");

        String r1 = "{\"id\": 1, \"name\":\"Marvin\"}";
        String r2 = "{\"id\": 2, \"name\":\"Slartibartfast\"}";
        List<String> urls = List.of(upload("blobs", r1), upload("blobs", r2));

        execute("copy names from ?", new Object[]{urls});
        assertThat(response).hasRowCount(2L);
        execute("refresh table names");
        execute("select name from names order by id");
        assertThat(printedTable(response.rows())).isEqualTo("Marvin\nSlartibartfast\n");
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
        assertThat(response).hasRowCount(2L);
        execute("refresh table names");
        execute("select name from names order by id");
        assertThat(printedTable(response.rows())).isEqualTo("Arthur\nSlartibartfast\n");
    }

    @Test
    public void testCopyFromIntoTableWithClusterBy() throws Exception {
        execute("create table quotes (id int, quote string) " +
            "clustered by (id)" +
            "with (number_of_replicas = 0)");

        execute("copy quotes from ? with (shared = true)", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select quote from quotes where id = 2");
        assertThat((String) response.rows()[0][0]).contains("lot of time");
    }

    @Test
    public void testCopyFromIntoTableWithPkAndClusterBy() throws Exception {
        execute("create table quotes (id int primary key, quote string) " +
            "clustered by (id)" +
            "with (number_of_replicas = 0)");

        execute("copy quotes from ?", new Object[]{copyFilePath + "test_copy_from.json"});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("select quote from quotes where id = 3");
        assertThat((String) response.rows()[0][0]).contains("Time is an illusion.");
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
    public void testCopyFromSymlinkFolderWithWildcard() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "*"
        });
        assertThat(response).hasRowCount(3L);
    }

    @Test
    public void testCopyFromSymlinkFolderWithPrefixedWildcard() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "d*"
        });
        assertThat(response).hasRowCount(3L);
    }

    @Test
    public void testCopyFromSymlinkFolderWithSuffixedWildcard() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "*.json"
        });
        assertThat(response).hasRowCount(3L);
    }

    @Test
    public void testCopyFromFileInSymlinkFolder() throws Exception {
        Path link = setUpTableAndSymlink("t");
        execute("copy t from ? with (shared=true)", new Object[]{
            link.toUri().toString() + "data.json"
        });
        assertThat(response).hasRowCount(3L);
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

        Path path = tmpFileWithLines(Arrays.asList(
            "{\"guid\": \"a\", \"ts\": 1496275200000}",
            "{\"guid\": \"b\", \"ts\": 1496275300000}"
        ));
        execute("copy t1 from ? with (shared=true)", new Object[] { path.toUri().toString() + "*.json"});
        assertThat(response).hasRowCount(2L);

        execute("copy t1 partition (g_ts_month = 1496275200000) from ? with (shared=true, overwrite_duplicates=true)",
            new Object[] { path.toUri().toString() + "*.json"});
        assertThat(response).hasRowCount(2L);
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
        assertThat(result).contains("| 2| 0| {}");
        // one of the first files will have a duplicate key error
        assertThat(result).contains("| 1| 1| {A document with the same primary key exists already={count=1, line_numbers=[");
        // file `data3.json` has a invalid timestamp error
        assertThat(result).contains("data3.json| 1| 2| {Cannot cast value ");
        assertThat(result).contains("Cannot cast value `Juli` to type `timestamp with time zone`={count=1, line_numbers=[3]}");
        assertThat(result).contains("Cannot cast value `May` to type `timestamp with time zone`={count=1, line_numbers=[2]}");
        // file `data4.json` has an invalid json item entry
        assertThat(result).contains("data4.json| 1| 1| {JSON parser error: ");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCopyFromReturnSummaryWithFailedURI() throws Exception {
        execute("create table t1 (id int primary key, ts timestamp with time zone)");

        Path tmpDir = newTempDir(LifecycleScope.TEST);
        String tmpDirStr = tmpDir.toUri().toString();

        String filename = "nonexistingfile.json";
        execute("copy t1 from ? return summary", new Object[]{tmpDirStr + filename});
        assertThat(response).hasRowCount((long) cluster().numDataNodes());

        boolean isRunningOnWindows = System.getProperty("os.name").startsWith("Windows");
        String expected = isRunningOnWindows
            ? "(The system cannot find the file specified)"
            : "(No such file or directory)";
        for (Object[] row : response.rows()) {
            assertThat((String) row[1]).endsWith(filename);
            assertThat(row[2]).isNull();
            assertThat(row[3]).isNull();
            assertThat(((Map<String, Object>) row[4]).keySet())
                .anySatisfy(key -> assertThat(key).contains(expected));
        }

        // with shared=true, only 1 data node must try to process the uri
        execute("copy t1 from ? with (shared=true) return summary", new Object[]{tmpDirStr + filename});
        assertThat(response).hasRowCount(1L);

        for (Object[] row : response.rows()) {
            assertThat((String) row[1]).endsWith(filename);
            assertThat(row[2]).isNull();
            assertThat(row[3]).isNull();
            assertThat(((Map<String, Object>) row[4]).keySet())
                .anySatisfy(key -> assertThat(key).contains(expected));
        }

        // with shared=true and wildcards all nodes will try to match a file
        filename = "*.json";
        execute("copy t1 from ? with (shared=true) return summary", new Object[] {tmpDirStr + filename});
        assertThat(response).hasRowCount((long) cluster().numDataNodes());

        for (Object[] row : response.rows()) {
            assertThat((String) row[1]).endsWith("*.json");
            assertThat(row[2]).isNull();
            assertThat(row[3]).isNull();
            assertThat(((Map<String, Object>) row[4]).keySet())
                .anySatisfy(key -> assertThat(key).contains("Cannot find any URI matching:"));
        }
    }

    @Test
    public void test_copy_from_csv_file_with_empty_string_as_null_property() throws Exception {
        execute(
            "CREATE TABLE t (id int primary key, name text) " +
            "CLUSTERED INTO 1 SHARDS ");
        File file = folder.newFile(UUID.randomUUID().toString());

        List<String> lines = List.of(
            "id,name",
            "1, \"foo\"",
            "2,\"\"",
            "3,"
        );
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute(
            "COPY t FROM ? WITH (format='csv', empty_string_as_null=true)",
            new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(response).hasRowCount(3L);
        refresh();

        execute("SELECT * FROM t ORDER BY id");
        assertThat(response).hasRows(
            "1| foo",
            "2| NULL",
            "3| NULL"
        );
    }

    @Test
    public void test_can_import_data_requiring_cast_from_csv_into_partitioned_table() throws Exception {
        execute(
            """
                    create table tbl (
                        ts timestamp with time zone not null,
                        ts_month timestamp with time zone generated always as date_trunc('month', ts)
                    ) partitioned by (ts_month)
                """);
        List<String> lines = List.of(
            "ts",
            "1626188198073"
        );
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("COPY tbl FROM ? WITH (format = 'csv', shared = true)",
                new Object[] {Paths.get(file.toURI()).toUri().toString()}
        );
        execute("refresh table tbl");
        execute("SELECT * FROM tbl");
        assertThat(response).hasRows(
            "1626188198073| 1625097600000"
        );
    }

    @UseNewCluster
    @Test
    public void test_copy_excludes_partitioned_values_from_source() throws Exception {
        execute("create table tbl (x int, p int) partitioned by (p)");

        {
            List<String> lines = List.of(
                """
                {"x": 10, "p": 1}
                """
            );
            File file = folder.newFile(UUID.randomUUID().toString());
            Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
            execute(
                "copy tbl from ? with (wait_for_completion = true, shared = true)",
                new Object[] { file.toPath().toUri().toString() }
            );
            execute("refresh table tbl");
            execute("SELECT _raw, * FROM tbl");
            assertThat(response).hasRows(
                "{\"1\":10}| 10| 1"
            );
        }

        {
            execute("create table tbl2 (x int, o object as (p int)) partitioned by (o['p'])");
            List<String> lines = List.of(
                """
                {"x": 10, "o": {"p": 1}}
                """
            );
            File file = folder.newFile(UUID.randomUUID().toString());
            Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
            execute(
                "copy tbl2 from ? with (wait_for_completion = true, shared = true)",
                new Object[] { file.toPath().toUri().toString() }
            );
            execute("refresh table tbl2");
            execute("SELECT _raw, * FROM tbl2");
            assertThat(response).hasRows(
                "{\"3\":10,\"4\":{}}| 10| {p=1}"
            );
        }
    }

    @Test
    public void test_copy_from_unknown_column_to_strict_object() throws Exception {
        // test for strict_table ver. can be found at FromRawInsertSourceTest, whereas strict_object is tested by DocumentMapper
        execute("create table t (o object(strict) as (a int))");

        List<String> lines = List.of(
            "{\"o\": {\"a\":123, \"b\":456}}"
        );
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? return summary", new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(printedTable(response.rows())).contains(
            "Cannot add column `b` to strict object `o`");
    }

    @Test
    public void test_copy_from_unknown_column_to_dynamic_object() throws Exception {
        execute("create table t (o object(dynamic) as (a int))");

        List<String> lines = List.of(
            "{\"o\": {\"a\":123, \"b\":456}}"
        );
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? with (shared = true)", new Object[]{Paths.get(file.toURI()).toUri().toString()});
        execute("refresh table t");
        execute("select o['a'] + o['b'] from t");
        assertThat(printedTable(response.rows())).isEqualTo("579\n");
    }

    @Test
    public void testCopyFromWithValidationSetToTrueDoesTypeValidation() throws Exception {

        // copying an empty string to a boolean column

        execute("create table t (a boolean)");

        List<String> lines = List.of("{\"a\": \"\"}");
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? with (shared = true, validation = true) return summary",
                new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(printedTable(response.rows())).contains("Cannot cast value `` to type `boolean`");
        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testCopyFromWithValidationSetToFalseStillValidatesIfGeneratedColumnsInvolved() throws Exception {
        execute("create table t (a boolean, b int generated always as 1)");

        List<String> lines = List.of("{\"a\": \"\"}");
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? with (shared = true, validation = false) return summary",
                new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(printedTable(response.rows())).contains("Cannot cast value `` to type `boolean`");
        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void testCopyFromWithValidationSetToFalseAndInsertingToPartitionedByColumn() throws Exception {
        // copying an empty string to a boolean column

        execute("create table t (a boolean, b boolean) partitioned by (b)");

        List<String> lines = List.of("{\"b\": \"\"}");
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? with (shared = true, validation = false) return summary",
                new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(printedTable(response.rows())).contains(
            // The validation should be skipped but since it is partitioned by column, that is used to create shards,
            // the values cannot stay raw. Notice that the error message is different from,
            // Cannot cast value `` to type `boolean`
            "Can't convert \"\" to boolean={count=1, line_numbers=[1]}"
        );
        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void test_validation_set_to_false_has_no_effect_and_results_in_validation_errors() throws Exception {
        // copying an empty string to a boolean column

        execute("create table t (a boolean, b boolean) partitioned by (b)");

        List<String> lines = List.of("{\"a\": \"\"}"); // a
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? with (shared = true, validation = false) return summary",
                new Object[]{Paths.get(file.toURI()).toUri().toString()});

        assertThat(response.rows()[0][4].toString()).contains("Cannot cast value `` to type `boolean`");
    }

    @Test
    public void testCopyFromWithValidationSetToFalseStillValidatesIfDefaultExpressionsInvolved() throws Exception {
        execute("create table t (a boolean, b int default 1)");

        List<String> lines = List.of("{\"a\": \"\"}");
        File file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);

        execute("copy t from ? with (shared = true, validation = false) return summary",
                new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(printedTable(response.rows())).contains("Cannot cast value `` to type `boolean`");
        execute("refresh table t");
        execute("select count(*) from t");
        assertThat(response.rows()[0][0]).isEqualTo(0L);
    }

    @Test
    public void test_copy_from_fail_gracefully_in_case_of_invalid_data() throws Exception {
        execute("create table t (obj object(dynamic) as (x int))");

        // Checking out all variants from https://github.com/crate/crate/issues/12201#issuecomment-1072472337

        // Intentionally "sandwiching" valid line between 2 invalids as there used to be test failure depending on the valid/invalid order.
        List<String> lines = List.of(
            "obj\n",
            "1,2\n",
            "\"{\"\"x\"\":1}\"\n",   // "{""x"":1}" - works
            "3,4\n",
            "\"{\"x\":1}\"\n",       // "{"x":1}"
            "\"'{\"\"x\"\":1}'\"\n", // "'{""x"":1}'"
            "\"{\"x\" = 1}\"\n",     // "{"x" = 1}"
            "\"{\"\"x\"\" = 1}\"\n", // "{""x"" = 1}"
            "{\"\"x\"\":1}\n",       // {""x"":1}
            "{\"x\":1}\n",           // {"x":1} - works
            "{\"x\" = 1}\n",         // {"x" = 1}
            "{x = 1}\n"              // {x = 1}
        );

        File file = folder.newFile(UUID.randomUUID() + ".csv");
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        execute("copy t from ? with (shared = true) return summary", new Object[]{Paths.get(file.toURI()).toUri().toString()});
        assertThat(response.rows()[0][2]).isEqualTo(2L);
        assertThat(response.rows()[0][3]).isEqualTo(9L);
    }

    @Test
    public void test_copy_preserves_implied_top_level_column_order() throws IOException {
        execute(
            """
                create table t (
                    p int
                ) partitioned by (p) with (column_policy = 'dynamic');
                """
        );
        var lines = List.of(
            """
            {"b":1, "a":1, "d":1, "c":1}
            """
        );
        var file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        execute("copy t from ? ", new Object[]{Paths.get(file.toURI()).toUri().toString()});
        execute("refresh table t");
        execute("select * from t");
        assertThat(response.cols())
            // follow the same order as provided by '{"b":1, "a":1, "d":1, "c":1}'
            .isEqualTo(new String[] {"p", "b", "a", "d", "c"});
    }

    @Test
    public void test_copy_preserves_the_implied_sub_column_order() throws IOException {
        execute(
            """
                create table doc.t (
                    p int,
                    o object
                ) partitioned by (p) with (column_policy = 'dynamic');
                """
        );
        var lines = List.of(
            """
            {"o":{"c":1, "a":{"d":1, "b":1, "c":1, "a":1}, "b":1}}
            """
        );
        var file = folder.newFile(UUID.randomUUID().toString());
        Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        execute("copy doc.t from ? ", new Object[]{Paths.get(file.toURI()).toUri().toString()});
        execute("refresh table doc.t");
        execute("show create table doc.t");
        assertThat(printedTable(response.rows()))
            // follow the same order as provided by '{"o":{"c":1, "a":{"d":1, "b":1, "c":1, "a":1}, "b":1}}'
            .contains(
                "CREATE TABLE IF NOT EXISTS \"doc\".\"t\" (\n" +
                "   \"p\" INTEGER,\n" +
                "   \"o\" OBJECT(DYNAMIC) AS (\n" +
                "      \"c\" BIGINT,\n" +
                "      \"a\" OBJECT(DYNAMIC) AS (\n" +
                "         \"d\" BIGINT,\n" +
                "         \"b\" BIGINT,\n" +
                "         \"c\" BIGINT,\n" +
                "         \"a\" BIGINT\n" +
                "      ),\n" +
                "      \"b\" BIGINT\n" +
                "   )\n" +
                ")"
            );
    }

    @Test
    public void test_generated_non_deterministic_value_is_consistent_on_primary_and_replica() throws Exception {
        execute("""
            create table tbl (x int, created generated always as round((random() + 1) * 100))
            clustered into 1 shards
            with (number_of_replicas = 1)
            """
        );
        Path path = tmpFileWithLines(Arrays.asList(
            "{\"x\": 1}"
        ));
        execute("copy tbl from ? with (shared=true)", new Object[] { path.toUri().toString() + "*.json"});

        execute("refresh table tbl");
        execute("select x, created from tbl");

        // (int) response.rows()[0][0] used to be null because replica
        // used regular Indexer instead of RawIndexer
        // with values ["{\"x\": 1}"][some_long_timestamp], ie tried to write String value into int column.

        int x = (int) response.rows()[0][0];
        long created = (long) response.rows()[0][1];

        // some iterations to ensure it hits both primary and replica
        for (int i = 0; i < 30; i++) {
            execute("select x, created from tbl").rows();
            assertThat(response.rows()[0][0]).isEqualTo(x);
            assertThat(response.rows()[0][1]).isEqualTo(created);
        }
    }
}
