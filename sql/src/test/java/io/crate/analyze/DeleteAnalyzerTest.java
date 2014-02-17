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
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operator.operator.EqOperator;
import io.crate.operator.operator.OperatorModule;
import io.crate.operator.reference.sys.node.NodeLoadExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DoubleLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.common.inject.Module;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class DeleteAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindReferences() {
            super.bindReferences();
            referenceBinder.addBinding(LOAD_INFO.ident()).to(NodeLoadExpression.class).asEagerSingleton();
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new TestModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testDeleteWhere() throws Exception {
        Statement statement = SqlParser.createStatement("delete from sys.nodes where load['1'] = 1");
        SelectAnalysis analysis = (SelectAnalysis) analyzer.analyze(statement);
        assertTrue(analysis.isDelete());
        assertEquals(SysNodesTableInfo.IDENT, analysis.table().ident());

        assertThat(analysis.rowGranularity(), is(RowGranularity.NODE));
        assertFalse(analysis.hasGroupBy());

        Function whereClause = analysis.whereClause();
        assertEquals(EqOperator.NAME, whereClause.info().ident().name());
        assertFalse(whereClause.info().isAggregate());

        assertThat(whereClause.arguments().get(0), IsInstanceOf.instanceOf(Reference.class));
        assertThat(whereClause.arguments().get(1), IsInstanceOf.instanceOf(DoubleLiteral.class));

    }
}
