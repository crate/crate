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

package io.crate.metadata;

import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.reference.StaticTableReferenceResolver;
import io.crate.expression.reference.sys.node.NodeStatsContext;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.Version;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.core.Is.is;

public class SysNodesTableInfoTest extends CrateUnitTest {

    /**
     * Ensures that all columns registered in SysNodesTableInfo can actually be resolved
     */
    @Test
    public void testRegistered() {
        SysNodesTableInfo info = new SysNodesTableInfo();
        ReferenceResolver<?> referenceResolver = new StaticTableReferenceResolver<>(SysNodesTableInfo.expressions());
        Iterator<Reference> iter = info.iterator();
        while (iter.hasNext()) {
            assertNotNull(referenceResolver.getImplementation(iter.next()));
        }
    }

    @Test
    public void testCompatibilityVersion() {
        RowCollectExpressionFactory<NodeStatsContext> sysNodeTableStatws = SysNodesTableInfo.expressions().get(
            SysNodesTableInfo.Columns.VERSION);

        assertThat(sysNodeTableStatws.create().getChild("minimum_index_compatibility_version").value(),
                   is(Version.CURRENT.minimumIndexCompatibilityVersion().externalNumber()));

        assertThat(sysNodeTableStatws.create().getChild("minimum_wire_compatibility_version").value(),
                   is(Version.CURRENT.minimumCompatibilityVersion().externalNumber()));
    }
}
