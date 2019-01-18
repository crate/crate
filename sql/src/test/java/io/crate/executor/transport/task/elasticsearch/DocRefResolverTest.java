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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.Lists;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.crate.testing.TestingHelpers.refInfo;
import static org.hamcrest.Matchers.is;

public class DocRefResolverTest extends CrateUnitTest {

    private static final BytesReference SOURCE = new BytesArray("{\"x\": 1}".getBytes());
    private static final DocRefResolver REF_RESOLVER =
        new DocRefResolver(Collections.emptyList());
    private static final Doc GET_RESULT =
        new Doc("t1", "abc", 1L, XContentHelper.convertToMap(SOURCE, false, XContentType.JSON).v2(), SOURCE::utf8ToString);

    @Test
    public void testSystemColumnsCollectExpressions() throws Exception {
        List<Reference> references = Lists.newArrayList(
            refInfo("t1._id", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.ID), RowGranularity.DOC),
            refInfo("t1._version", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.VERSION), RowGranularity.DOC),
            refInfo("t1._doc", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.DOC), RowGranularity.DOC),
            refInfo("t1._raw", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.RAW), RowGranularity.DOC)
        );

        List<CollectExpression<Doc, ?>> collectExpressions = new ArrayList<>(4);
        for (Reference reference : references) {
            CollectExpression<Doc, ?> collectExpression = REF_RESOLVER.getImplementation(reference);
            collectExpression.setNextRow(GET_RESULT);
            collectExpressions.add(collectExpression);
        }

        assertThat(collectExpressions.get(0).value(), is("abc"));
        assertThat(collectExpressions.get(1).value(), is(1L));
        assertThat(collectExpressions.get(2).value(), is(XContentHelper.convertToMap(SOURCE, false, XContentType.JSON).v2()));
        assertThat(collectExpressions.get(3).value(), is(SOURCE.utf8ToString()));
    }
}
