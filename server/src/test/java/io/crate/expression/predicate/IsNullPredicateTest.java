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

package io.crate.expression.predicate;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.analyze.relations.DocTableRelation;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.GenericFunctionQuery;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.testing.IndexEnv;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class IsNullPredicateTest extends ScalarTestCase {

    @Test
    public void test_refExistsQuery_does_not_throw_npe_on_missing_child_reference_of_object_type() throws Exception {
        ObjectType type = ObjectType.builder()
            .setInnerType("x", DataTypes.INTEGER)
            .build();
        LuceneQueryBuilder luceneQueryBuilder = new LuceneQueryBuilder(sqlExpressions.nodeCtx);
        Symbol query = sqlExpressions.asSymbol("obj is NULL");
        DocTableRelation relation = (DocTableRelation) tableSources.get(new RelationName("doc", "users"));
        DocTableInfo table = relation.tableInfo();
        try (var indexEnv = new IndexEnv(
                THREAD_POOL,
                table,
                clusterService.state(),
                Version.CURRENT)) {
            Context context = luceneQueryBuilder.convert(
                query,
                txnCtx,
                indexEnv.mapperService(),
                indexEnv.indexService().index().getName(),
                indexEnv.queryShardContext(),
                table,
                indexEnv.queryCache()
            );
            SimpleReference ref = new SimpleReference(
                new ReferenceIdent(table.ident(), "obj"),
                RowGranularity.DOC,
                type,
                1,
                null
            );
            Query refExistsQuery = IsNullPredicate.refExistsQuery(ref, context, true);
            assertThat(refExistsQuery).isNull();
        }
    }

    @Test
    public void test_refExistsQuery_falls_back_to_generic_function_query_on_sub_columns_of_ignored_objects() throws Exception {
        LuceneQueryBuilder luceneQueryBuilder = new LuceneQueryBuilder(sqlExpressions.nodeCtx);
        DocTableRelation relation = (DocTableRelation) tableSources.get(new RelationName("doc", "users"));
        DocTableInfo table = relation.tableInfo();
        try (var indexEnv = new IndexEnv(
            THREAD_POOL,
            table,
            clusterService.state(),
            Version.CURRENT)) {
            Query query = luceneQueryBuilder.convert(
                sqlExpressions.asSymbol("obj_ignored['x'] is NULL"),
                txnCtx,
                indexEnv.mapperService(),
                indexEnv.indexService().index().getName(),
                indexEnv.queryShardContext(),
                table,
                indexEnv.queryCache()
            ).query();
            assertThat(query).isExactlyInstanceOf(GenericFunctionQuery.class);
            assertThat(query.toString()).isEqualTo("(_doc['obj_ignored']['x'] IS NULL)");
        }
    }

    @Test
    public void testNormalizeSymbolFalse() throws Exception {
        assertNormalize("'a' is null", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolTrue() throws Exception {
        assertNormalize("null is null", isLiteral(true));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("name is null", isFunction(IsNullPredicate.NAME));
    }

    @Test
    public void testNormalizeUndefinedType() {
        assertNormalize("obj_ignored['column'] is null", isFunction(IsNullPredicate.NAME));
    }

    @Test
    public void testEvaluateWithInputThatReturnsNull() throws Exception {
        assertEvaluate("name is null", true, Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void test_normalize_not_null_ref_to_false() {
        assertNormalize("b is null", isLiteral(false));
    }
}

