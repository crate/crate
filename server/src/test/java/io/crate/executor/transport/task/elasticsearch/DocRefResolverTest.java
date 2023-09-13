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

package io.crate.executor.transport.task.elasticsearch;

import static io.crate.testing.TestingHelpers.refInfo;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.reference.Doc;
import io.crate.expression.reference.DocRefResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.server.xcontent.XContentHelper;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class DocRefResolverTest extends ESTestCase {

    private static final BytesReference SOURCE = new BytesArray("{\"x\": 1}".getBytes());
    private static final DocRefResolver REF_RESOLVER =
        new DocRefResolver(Collections.emptyList());
    private static final Doc GET_RESULT = new Doc(2,
                                                  "t1",
                                                  "abc",
                                                  1L,
                                                  1L,
                                                  1L,
                                                  XContentHelper.convertToMap(SOURCE, false, XContentType.JSON).map(),
                                                  SOURCE::utf8ToString);

    @Test
    public void testSystemColumnsCollectExpressions() throws Exception {
        List<Reference> references = List.of(
            refInfo("t1._id", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.ID), RowGranularity.DOC),
            refInfo("t1._version", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.VERSION), RowGranularity.DOC),
            refInfo("t1._doc", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.DOC), RowGranularity.DOC),
            refInfo("t1._raw", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.RAW), RowGranularity.DOC),
            refInfo("t1._docid", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.DOCID), RowGranularity.DOC),
            refInfo("t1._seq_no", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.SEQ_NO), RowGranularity.DOC),
            refInfo("t1._primary_term", DocSysColumns.COLUMN_IDENTS.get(DocSysColumns.PRIMARY_TERM), RowGranularity.DOC)
        );

        List<CollectExpression<Doc, ?>> collectExpressions = new ArrayList<>(4);
        for (Reference reference : references) {
            CollectExpression<Doc, ?> collectExpression = REF_RESOLVER.getImplementation(reference);
            collectExpression.setNextRow(GET_RESULT);
            collectExpressions.add(collectExpression);
        }

        assertThat(collectExpressions.get(0).value()).isEqualTo("abc");
        assertThat(collectExpressions.get(1).value()).isEqualTo(1L);
        assertThat(collectExpressions.get(2).value()).isEqualTo(XContentHelper.convertToMap(SOURCE, false, XContentType.JSON).map());
        assertThat(collectExpressions.get(3).value()).isEqualTo(SOURCE.utf8ToString());
        assertThat(collectExpressions.get(4).value()).isEqualTo(2);
        assertThat(collectExpressions.get(5).value()).isEqualTo(1L);
        assertThat(collectExpressions.get(6).value()).isEqualTo(1L);
    }

    // https://github.com/crate/crate/issues/13990
    @Test
    public void test_convert_empty_or_null_arrays_added_dynamically_to_nulls() {
        Doc doc = new Doc(0, "t", "1", 1L, 0L, 1L,
                          // only useful values here
                          Map.of("a", 1,
                              "x", List.of(), // empty array
                                 "y", Collections.nCopies(3, null), // array of nulls
                                 "o", Map.of("oo", Map.of("oox", List.of(), // nested empty array
                                             "ooy", Collections.nCopies(3, null)))), // nested array of nulls
                          () -> {
                              throw new AssertionError("this should not be used");
                          });

        Reference x = new SimpleReference(
            new ReferenceIdent(
                new RelationName(null, "doc"),
                new ColumnIdent("x")),
            RowGranularity.DOC,
            DataTypes.STRING, // as long as this is not an array, the test should pass
            0,
            null);
        var collectExpression = REF_RESOLVER.getImplementation(x);
        collectExpression.setNextRow(doc);
        assertThat(collectExpression.value()).isNull();

        Reference y = new SimpleReference(
            new ReferenceIdent(
                new RelationName(null, "doc"),
                new ColumnIdent("y")),
            RowGranularity.DOC,
            DataTypes.UNTYPED_OBJECT, // as long as this is not an array, the test should pass
            0,
            null);
        collectExpression = REF_RESOLVER.getImplementation(y);
        collectExpression.setNextRow(doc);
        assertThat(collectExpression.value()).isNull();

        ObjectType ot = new ObjectType.Builder()
            .setInnerType("oo", new ObjectType.Builder()
                                        .setInnerType("oox", DataTypes.NUMERIC)
                                        .setInnerType("ooy", DataTypes.NUMERIC).build())
            .build();
        Reference o = new SimpleReference(
            new ReferenceIdent(
                new RelationName(null, "doc"),
                new ColumnIdent("o")),
            RowGranularity.DOC,
            ot,
            0,
            null);
        collectExpression = REF_RESOLVER.getImplementation(o);
        collectExpression.setNextRow(doc);
        var map = (Map<?, ?>) collectExpression.value();
        assertThat(map).hasSize(1);
        var nestedMap = (Map<?, ?>) map.get("oo");
        assertThat(nestedMap).hasSize(2);
        assertThat(nestedMap.get("ox")).isNull();
        assertThat(nestedMap.get("oy")).isNull();
    }
}
