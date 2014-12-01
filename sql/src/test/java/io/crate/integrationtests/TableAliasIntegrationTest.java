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
import io.crate.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TableAliasIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private String tableAliasSetup() throws Exception {
        String tableName = "mytable";
        String tableAlias = "mytablealias";
        execute(String.format("create table %s (id integer primary key, " +
                        "content string)",
                tableName
        ));
        ensureGreen();
        client().admin().indices().prepareAliases().addAlias(tableName,
                tableAlias).execute().actionGet();
        refresh();
        Thread.sleep(20);
        return tableAlias;
    }

    @Test
    public void testSelectTableAlias() throws Exception {
        execute("create table quotes_en (id int primary key, quote string) with (number_of_replicas=0)");
        execute("create table quotes_de (id int primary key, quote string) with (number_of_replicas=0)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();

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
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testSelectTableAliasSchemaExceptionColumnDataType() throws Exception {
        execute("create table quotes_en (id int primary key, quote int) with (number_of_replicas=0)");
        execute("create table quotes_de (id int primary key, quote string) with (number_of_replicas=0)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testSelectTableAliasSchemaExceptionPrimaryKeyRoutingColumn() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int, quote string)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testSelectTableAliasSchemaExceptionIndices() throws Exception {
        execute("create table quotes_en (id int primary key, quote string)");
        execute("create table quotes_de (id int primary key, quote2 string index using fulltext)");
        client().admin().indices().prepareAliases().addAlias("quotes_en", "quotes")
                .addAlias("quotes_de", "quotes").execute().actionGet();
        ensureGreen();

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias \"quotes\" contains tables with different schema");
        execute("select quote from quotes where id = ?", new Object[]{1});
    }

    @Test
    public void testCountWithGroupByTableAlias() throws Exception {
        execute("create table characters_guide (race string, gender string, name string)");
        ensureGreen();
        execute("insert into characters_guide (race, gender, name) values ('Human', 'male', 'Arthur Dent')");
        execute("insert into characters_guide (race, gender, name) values ('Android', 'male', 'Marving')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Jeltz')");
        execute("insert into characters_guide (race, gender, name) values ('Vogon', 'male', 'Kwaltz')");
        refresh();

        execute("create table characters_life (race string, gender string, name string)");
        ensureGreen();
        execute("insert into characters_life (race, gender, name) values ('Rabbit', 'male', 'Agrajag')");
        execute("insert into characters_life (race, gender, name) values ('Human', 'male', 'Ford Perfect')");
        execute("insert into characters_life (race, gender, name) values ('Human', 'female', 'Trillian')");
        refresh();

        client().admin().indices().prepareAliases().addAlias("characters_guide", "characters")
                .addAlias("characters_life", "characters").execute().actionGet();
        ensureGreen();

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
        expectedException.expectMessage("The table 'mytablealias' already exists.");

        execute(String.format("create table %s (content string index off)", tableAlias));
    }

    @Test
    public void testDropTableWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Table alias not allowed in DROP TABLE statement.");
        execute(String.format("drop table %s", tableAlias));
    }

    @Test
    public void testCopyFromWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("aliases are read only");

        execute(String.format("copy %s from '/tmp/file.json'", tableAlias));

    }

    @Test
    public void testInsertWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("aliases are read only");

        execute(
                String.format("insert into %s (id, content) values (?, ?)", tableAlias),
                new Object[]{1, "bla"}
        );
    }

    @Test
    public void testUpdateWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("relation \"TableRelation{table=doc.mytablealias}\" is read-only and cannot be updated");

        execute(String.format("update %s set id=?, content=?", tableAlias), new Object[]{1, "bla"});
    }

    @Test
    public void testDeleteWithTableAlias() throws Exception {
        String tableAlias = tableAliasSetup();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("relation \"TableRelation{table=doc.mytablealias}\" is read-only and cannot be deleted");

        execute(String.format("delete from %s where id=?", tableAlias), new Object[]{1});
    }


    @Test
    public void testPartitionedTableKeepsAliasAfterSchemaUpdate() throws Exception {
        execute("create table t (name string, p string) partitioned by (p) " +
                "clustered into 2 shards with (number_of_replicas = 0)");
        ensureGreen();

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
                client().admin().indices().prepareGetTemplates(".partitioned.t.").execute().actionGet();
        IndexTemplateMetaData indexTemplateMetaData = indexTemplatesResponse.getIndexTemplates().get(0);
        AliasMetaData t = indexTemplateMetaData.aliases().get("t");
        assertThat(t.alias(), is("t"));

        execute("select partitioned_by from information_schema.tables where table_name = 't'");
        assertThat(((String[]) response.rows()[0][0])[0], is("p"));
    }

}
