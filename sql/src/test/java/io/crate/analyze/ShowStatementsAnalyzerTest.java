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

package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Reference;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.LikeOperator;
import io.crate.operation.operator.OperatorModule;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowTables;
import io.crate.testing.MockedClusterServiceModule;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Module;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShowStatementsAnalyzerTest extends BaseAnalyzerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static final TableIdent SCHEMATA = new TableIdent(InformationSchemaInfo.NAME, "schemata");
    public static final TableInfo SCHEMATA_INFO = new TestingTableInfo.Builder(
            SCHEMATA, new Routing(ImmutableMap.<String, Map<String, List<Integer>>>of()))
            .add("schema_name", DataTypes.STRING, null)
            .build();

    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(SCHEMATA.name())).thenReturn(SCHEMATA_INFO);
            schemaBinder.addBinding(InformationSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new OperatorModule()
        ));
        return modules;
    }

    public static class ShowStatementRewriterTest {
        ShowStatementAnalyzer.ShowStatementRewriter instance;

        @Before
        public void setUp() {
            instance = new ShowStatementAnalyzer.ShowStatementRewriter();
        }

        @Test
        public void testVisitShowTablesSchema() throws Exception {
            assertEquals(instance.visitShowTables(new ShowTables(new QualifiedName("QNAME")), null),
                    "SELECT distinct(table_name) as table_name " +
                    "FROM information_schema.tables " +
                    "WHERE schema_name = 'QNAME' " +
                    "ORDER BY 1");


            assertEquals(instance.visitShowTables(new ShowTables(null), null),
                    "SELECT distinct(table_name) as table_name " +
                    "FROM information_schema.tables " +
                    "WHERE schema_name NOT IN ('sys', 'information_schema') " +
                    "ORDER BY 1");
        }

        @Test
        public void testVisitShowTablesLike() throws Exception {
            assertEquals(instance.visitShowTables(new ShowTables(new QualifiedName("QNAME"), "likePattern"), null),
                    "SELECT distinct(table_name) as table_name " +
                    "FROM information_schema.tables " +
                    "WHERE schema_name = 'QNAME' AND table_name LIKE 'likePattern' " +
                    "ORDER BY 1");

            // SQL INJECTION
            assertEquals(instance.visitShowTables(new ShowTables(null, "'OR 1=1 --"), null),
                    "SELECT distinct(table_name) as table_name " +
                    "FROM information_schema.tables " +
                    "WHERE schema_name NOT IN ('information_schema', 'sys') AND table_name LIKE 'OR 1=1' " +
                    "ORDER BY 1");

            assertEquals(instance.visitShowTables(new ShowTables(null, "%"), null),
                    "SELECT distinct(table_name) as table_name " +
                    "FROM information_schema.tables " +
                    "WHERE schema_name NOT IN ('information_schema', 'sys') AND table_name LIKE '%' " +
                    "ORDER BY 1");

        }



        @Test
        public void testVisitShowTablesWhere() throws Exception {
            fail("IMPLEMENT THIS");
        }


        @Test
        public void testVisitShowSchemas() throws Exception {
            String result = instance.visitShowSchemas(new ShowSchemas(null, null), null);

            assertEquals(result, "SELECT schema_name " +
                                 "FROM information_schema.schemata " +
                                 "ORDER BY schema_name");

        }

        @Test
        public void testVisitShowSchemasLike() throws Exception {

            String result = instance.visitShowSchemas(new ShowSchemas("%", null), null);

            assertEquals(result, "SELECT schema_name " +
                                 "FROM information_schema.schemata " +
                                 "WHERE schema_name LIKE '%' " +
                                 "ORDER BY schema_name");
        }

        @Test
        public void testVisitShowSchemasWhere() throws Exception {
            String result = instance.visitShowSchemas(new ShowSchemas(null, null), null);
            fail("IMPLEMENT THIS");
            assertEquals(result, "SELECT information_schema.schemata.schema_name " +
                                 "WHERE information_schema.schemata.schema_name like '%' " +
                                 "ORDER BY information_schema.schemata.schema_name");

        }
    }
}
