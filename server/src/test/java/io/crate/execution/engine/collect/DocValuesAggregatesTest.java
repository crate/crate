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

package io.crate.execution.engine.collect;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DocValuesAggregatesTest extends CrateDummyClusterServiceUnitTest {

    private Functions functions;
    private SqlExpressions e;

    private final Aggregation longSumAggregation = new Aggregation(
        Signature.aggregate(
            SumAggregation.NAME,
            DataTypes.LONG.getTypeSignature(),
            DataTypes.LONG.getTypeSignature()
        ),
        DataTypes.LONG,
        List.of(new InputColumn(0, DataTypes.LONG))
    );

    @Before
    public void setup() {
        functions = createNodeContext().functions();
        RelationName name = new RelationName(DocSchemaInfo.NAME, "tbl");
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            name,
            "create table tbl (x bigint)",
            clusterService);
        Map<RelationName, AnalyzedRelation> sources = Map.of(name, new TableRelation(tableInfo));
        e = new SqlExpressions(sources);
    }

    @Test
    public void test_create_aggregators_for_reference_and_doc_value_field_for_the_correct_field_type() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            fqn -> new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG),
            List.of(e.asSymbol("tbl.x")),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(aggregators, contains(instanceOf(SumAggregation.SumLong.class)));
    }

    @Test
    public void test_create_aggregators_for_cast_reference_returns_aggregator_only_if_it_is_cast_to_numeric() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            fqn -> new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG),
            List.of(e.asSymbol("tbl.x::real")),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(aggregators, is(nullValue()));

        aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            fqn -> new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG),
            List.of(e.asSymbol("tbl.x::numeric")),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(aggregators, contains(instanceOf(SumAggregation.SumLong.class)));
    }

    @Test
    public void test_create_aggregators_for_literal_aggregation_input_returns_null() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            fqn -> new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG),
            List.of(Literal.of(1)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(aggregators, is(nullValue()));
    }

    @Test
    public void test_create_aggregators_for_reference_not_mapped_to_field_returns_null() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            fqn -> null,
            List.of(Literal.of(1)),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(aggregators, is(nullValue()));
    }

    @Test
    public void test_create_aggregators_for_reference_and_without_doc_value_field_returns_null() {
        var field = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        field.setHasDocValues(false);
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            fqn -> field,
            List.of(e.asSymbol("tbl.x")),
            SearchPath.pathWithPGCatalogAndDoc()
        );
        assertThat(aggregators, is(nullValue()));
    }
}
