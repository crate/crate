/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.aggregation.impl.AggregationImplModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WhereClauseValidatorTest extends BaseAnalyzerTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    static class TestMetaDataModule extends MetaDataModule {

        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(ReferenceInfos.DEFAULT_SCHEMA_NAME).toInstance(schemaInfo);
        }

    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule(),
                new AggregationImplModule(),
                new PredicateModule(),
                new ScalarFunctionModule()
        ));
        return modules;
    }

    @Test
    public void testUpdateWhereVersionUsingWrongOperator() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("update users set text = ? where text = ? and \"_version\" >= ?",
                new Object[]{"already in panic", "don't panic", 3});
    }

    @Test
    public void testUpdateWhereVersionIsColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("update users set col2 = ? where _version = id",
                new Object[]{1});
    }

    @Test
    public void testUpdateWhereVersionInOperatorColumn() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("update users set col2 = ? where _version in (1,2,3)",
                new Object[]{1});
    }

    @Test
    public void testUpdateWhereVersionAddition() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("update users set col2 = ? where _version + 1 = 2",
                new Object[]{1});
    }

    @Test
    public void testSelectWhereVersionAddition() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("Select * from users where id = 1 and _version + 1 = 2");
    }

    @Test
    public void testSelectWhereVersionNotPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("update users set text = ? where not (_version = 1 and id = 1)",
                new Object[]{"Hello"});
    }

    @Test
    public void testSelectWhereVersionIsNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("update users set col2 = ? where _version is null",
                new Object[]{1});
    }

    @Test
    public void testDeleteWhereVersionIsNullPredicate() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Filtering \"_version\" in WHERE clause only works using the \"=\" operator, checking for a numeric value");
        analyze("delete from users where _version is null",
                new Object[]{1});
    }
}
