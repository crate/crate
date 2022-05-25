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

package io.crate.metadata;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;

public class SysNodesTableInfoTest extends CrateDummyClusterServiceUnitTest {

    /**
     * Ensures that all columns registered in SysNodesTableInfo can actually be resolved
     */
    @Test
    public void testRegistered() {
        var info = SysNodesTableInfo.create();
        ReferenceResolver<?> referenceResolver = new StaticTableReferenceResolver<>(info.expressions());
        for (SimpleReference reference : info) {
            assertNotNull(referenceResolver.getImplementation(reference));
        }
    }

    @Test
    public void testCompatibilityVersion() {
        RowCollectExpressionFactory<NodeStatsContext> sysNodeTableStatws = SysNodesTableInfo.create().expressions().get(
            SysNodesTableInfo.Columns.VERSION);

        assertThat(sysNodeTableStatws.create().getChild("minimum_index_compatibility_version").value(),
                   is(Version.CURRENT.minimumIndexCompatibilityVersion().externalNumber()));

        assertThat(sysNodeTableStatws.create().getChild("minimum_wire_compatibility_version").value(),
                   is(Version.CURRENT.minimumCompatibilityVersion().externalNumber()));
    }

    @Test
    public void test_column_that_is_a_child_of_an_array_has_array_type_on_select() {
        var table = SysNodesTableInfo.create();
        SimpleReference ref = table.getReference(new ColumnIdent("fs", List.of("data", "path")));
        assertThat(ref.valueType(), is(new ArrayType<>(DataTypes.STRING)));

        SQLExecutor e = SQLExecutor.builder(clusterService).build();
        AnalyzedRelation statement = e.analyze("select fs['data']['path'] from sys.nodes");
        assertThat(statement.outputs().get(0).valueType(), is(new ArrayType<>(DataTypes.STRING)));
    }

    @Test
    public void test_fs_data_is_a_object_array() {
        var table = SysNodesTableInfo.create();
        SimpleReference ref = table.getReference(new ColumnIdent("fs", "data"));
        assertThat(ref.valueType().id(), Matchers.is(ArrayType.ID));
        assertThat(((ArrayType<?>) ref.valueType()).innerType().id(), is(ObjectType.ID));
    }
}
