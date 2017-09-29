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

import io.crate.action.sql.SQLActionException;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class TableAliasIntegrationTest extends SQLTransportIntegrationTest {

    private String tableAliasSetup() throws Exception {
        String tableName = getFqn("mytable");
        String tableAlias = getFqn("mytablealias");
        execute(String.format(Locale.ENGLISH, "create table %s (id integer primary key, " +
                                              "content string)",
            tableName
        ));
        ensureYellow();
        client().admin().indices().prepareAliases().addAlias(tableName, tableAlias).execute().actionGet();
        refresh();
        Thread.sleep(20);
        return tableAlias;
    }

    @Test
    public void testSelectTableAlias() throws Exception {
        execute("create table quotes_en (id int primary key, quote string) with (number_of_replicas=0)");
        execute("create table quotes_de (id int primary key, quote string) with (number_of_replicas=0)");
        client().admin().indices().prepareAliases().addAlias(getFqn("quotes_en"), getFqn("quotes"))
            .addAlias(getFqn("quotes_de"), getFqn("quotes")).execute().actionGet();
        ensureYellow();

        execute("insert into quotes_en values (?,?)", new Object[]{1, "Don't panic"});
        assertEquals(1, response.rowCount());
        execute("insert into quotes_de values (?,?)", new Object[]{2, "Keine Panik"});
        assertEquals(1, response.rowCount());
        refresh();

        execute("select quote from quotes where id = ?", new Object[]{1});
        assertEquals(1, response.rowCount());
        execute("select quote from quotes where id = ?", new Object[]{2});
        assertEquals(1, response.rowCount());
    }

    @Test
    public void testSelectTableAliasSchemaExceptionColumnDefinition() throws Exception {
        execute("create table quotes_en (id int primary key, quote string, author string)");
        execute("create table quotes_de (id int primary key, quote2 string)");
        client().admin().indices().prepareAliases().addAlias(getFqn("quotes_en"), getFqn("quotes"))
            .addAlias(getFqn("quotes_de"), getFqn("quotes")).execute().actionGet();
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testSelectTableAliasSchemaExceptionColumnDataType() throws Exception {
        execute("create table quotes_en (id int primary key, quote int) with (number_of_replicas=0)");
        execute("create table quotes_de (id int primary key, quote string) with (number_of_replicas=0)");
        client().admin().indices().prepareAliases().addAlias(getFqn("quotes_en"), getFqn("quotes"))
            .addAlias(getFqn("quotes_de"), getFqn("quotes")).execute().actionGet();
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testSelectTableAliasSchemaExceptionPrimaryKeyRoutingColumn() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int, quote string)");
        client().admin().indices().prepareAliases().addAlias(getFqn("quotes_en"), getFqn("quotes"))
            .addAlias(getFqn("quotes_de"), getFqn("quotes")).execute().actionGet();
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testSelectTableAliasSchemaExceptionIndices() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int primary key, quote2 string index using fulltext)");
        client().admin().indices().prepareAliases().addAlias(getFqn("quotes_en"), getFqn("quotes"))
            .addAlias(getFqn("quotes_de"), getFqn("quotes")).execute().actionGet();
        ensureYellow();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testCountWithGroupByTableAlias() throws Exception {
        execute("create table characters_guide (race string, gender string, name string)");
        ensureYellow();
        execute("insert into characters_guide (race, gender, name) values ('Human', 'male', 'Arthur Dent')");
        execute("insert into characters_guide (race, gender, name) values ('Android', 'male', 'Marving')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Jeltz')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Kwaltz')");
        refresh();

        execute("create table characters_life (race string, gender string, name string)");
        ensureYellow();
        execute("insert into characters_life (race, gender, name) values ('Rabbit', 'male', 'Agrajag')");
        execute("insert into characters_life (race, gender, name) values ('Human', 'male', 'Ford Perfect')");
        execute("insert into characters_life (race, gender, name) values ('Human', 'female', 'Trillian')");
        refresh();

        client().admin().indices().prepareAliases().addAlias(getFqn("characters_guide"), getFqn("characters"))
            .addAlias(getFqn("characters_life"), getFqn("characters")).execute().actionGet();
        ensureYellow();

        execute("select count(*) from characters");
        assertEquals(7L, response.rows()[0][0]);

        execute("select count(*), race from characters group by race order by count(*) desc " +
                "limit 2");
        assertEquals(2, response.rowCount());
        assertEquals("Human", response.rows()[0][1]);
        assertEquals("Vogon", response.rows()[1][1]);
    }

    @Test
    public void testCreateTableWithExistingTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "The table '%s.mytablealias' already exists.", sqlExecutor.getDefaultSchema()));

        execute(String.format(Locale.ENGLISH, "create table %s (content string index off)", tableAlias));
    }

    @Test
    public void testDropTableWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "The relation \"%s.mytablealias\" doesn't support or allow DROP " +
                                        "operations, as it is read-only.", sqlExecutor.getDefaultSchema()));
        execute(String.format(Locale.ENGLISH, "drop table %s", tableAlias));
    }

    @Test
    public void testCopyFromWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "The relation \"%s.mytablealias\" doesn't support or allow INSERT " +
                                        "operations, as it is read-only.", sqlExecutor.getDefaultSchema()));

        execute(String.format(Locale.ENGLISH, "copy %s from '/tmp/file.json'", tableAlias));

    }

    @Test
    public void testInsertWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "The relation \"%s.mytablealias\" doesn't support or allow INSERT " +
                                        "operations, as it is read-only.", sqlExecutor.getDefaultSchema()));
        execute(
            String.format(Locale.ENGLISH, "insert into %s (id, content) values (?, ?)", tableAlias),
            new Object[]{1, "bla"}
        );
    }

    @Test
    public void testUpdateWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format(Locale.ENGLISH, "The relation \"%s.mytablealias\" doesn't support or allow UPDATE " +
                                        "operations, as it is read-only.", sqlExecutor.getDefaultSchema()));

        execute(String.format(Locale.ENGLISH, "update %s set id=?, content=?", tableAlias), new Object[]{1, "bla"});
    }

    @Test
    public void testDeleteWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(String.format("The relation \"%s.mytablealias\" doesn't support or allow DELETE " +
                                        "operations, as it is read-only.", sqlExecutor.getDefaultSchema()));

        execute(String.format(Locale.ENGLISH, "delete from %s where id=?", tableAlias), new Object[]{1});
    }


    @Test
    public void testPartitionedTableKeepsAliasAfterSchemaUpdate() throws Exception {
        execute("create table t (name string, p string) partitioned by (p) " +
                "clustered into 2 shards with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into t (name, p) values ('Arthur', 'a')");
        execute("insert into t (name, p) values ('Trillian', 'a')");
        execute("alter table t add column age integer");
        execute("insert into t (name, p) values ('Marvin', 'b')");
        waitNoPendingTasksOnAll();
        refresh();

        execute("select count(*) from t");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(3L));

        GetIndexTemplatesResponse indexTemplatesResponse =
            client().admin().indices().prepareGetTemplates(String.format(Locale.ENGLISH, "%s..partitioned.t.", sqlExecutor.getDefaultSchema()))
                .execute().actionGet();
        IndexTemplateMetaData indexTemplateMetaData = indexTemplatesResponse.getIndexTemplates().get(0);
        AliasMetaData t = indexTemplateMetaData.aliases().get(getFqn("t"));
        assertThat(t.alias(), is(getFqn("t")));

        execute("select partitioned_by from information_schema.tables where table_name = 't'");
        assertThat((String) ((Object[]) response.rows()[0][0])[0], is("p"));
    }

}
