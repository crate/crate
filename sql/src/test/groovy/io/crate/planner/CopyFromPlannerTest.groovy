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

package io.crate.planner

import io.crate.analyze.symbol.Literal
import io.crate.metadata.doc.DocSysColumns
import io.crate.planner.node.dql.Collect
import io.crate.planner.node.dql.FileUriCollectPhase
import io.crate.planner.projection.SourceIndexWriterProjection
import io.crate.testing.T3
import org.apache.lucene.util.BytesRef
import org.junit.Test

class CopyFromPlannerTest extends AbstractPlannerTest {

    @Test
    public void testCopyFromPlan() throws Exception {
        Collect plan =  plan("copy users from '/path/to/file.extension'");
        assert plan.collectPhase() instanceof FileUriCollectPhase;

        FileUriCollectPhase collectPhase = (FileUriCollectPhase)plan.collectPhase();
        assert ((Literal) collectPhase.targetUri()).value() == new BytesRef("/path/to/file.extension");
    }

    @Test
    public void testCopyFromNumReadersSetting() throws Exception {
        Collect plan = plan("copy users from '/path/to/file.extension' with (num_readers=1)");
        assert plan.collectPhase() instanceof FileUriCollectPhase
        FileUriCollectPhase collectPhase = (FileUriCollectPhase) plan.collectPhase();
        assert collectPhase.nodeIds().size() == 1
    }

    @Test
    public void testCopyFromPlanWithParameters() throws Exception {
        Collect collect = plan("copy users " +
                "from '/path/to/file.ext' with (bulk_size=30, compression='gzip', shared=true)");
        assert collect.collectPhase() instanceof FileUriCollectPhase
        FileUriCollectPhase collectPhase = (FileUriCollectPhase)collect.collectPhase();
        SourceIndexWriterProjection indexWriterProjection = (SourceIndexWriterProjection) collectPhase.projections().get(0);
        assert indexWriterProjection.bulkActions() == 30
        assert collectPhase.compression() == "gzip"
        assert collectPhase.sharedStorage()

        // verify defaults:
        collect = plan("copy users from '/path/to/file.ext'");
        collectPhase = (FileUriCollectPhase)collect.collectPhase();
        assert collectPhase.compression() == null;
        assert collectPhase.sharedStorage() == null;
    }

    @Test
    public void test_IdIsNotCollectedOrUsedAsClusteredBy() throws Exception {
        Collect collect = (Collect) plan("copy t1 from '/path/file.ext'");
        SourceIndexWriterProjection projection =
                (SourceIndexWriterProjection) collect.collectPhase().projections().get(0);
        assert projection.clusteredBy() == null;
        assert collect.collectPhase().toCollect() == [T3.T1_INFO.getReference(DocSysColumns.RAW)]
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCopyFromPlanWithInvalidParameters() throws Exception {
        plan("copy users from '/path/to/file.ext' with (bulk_size=-28)");
    }

    @Test
    public void testNodeFiltersNoMatch() throws Exception {
        Collect cm = (Collect) plan("copy users from '/path' with (node_filters={name='foobar'})");
        assert cm.collectPhase().nodeIds() == []
    }
}
