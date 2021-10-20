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

package io.crate.execution.engine.collect;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.aggregation.impl.SumAggregation;
import io.crate.execution.engine.aggregation.impl.templates.SortedNumericDocValueAggregator;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.functions.Signature;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class DocValuesAggregatesTest extends CrateDummyClusterServiceUnitTest {

    private Functions functions;
    private SqlExpressions e;
    private DocTableInfo table;

    private final Aggregation longSumAggregation = new Aggregation(
        Signature.aggregate(
            SumAggregation.NAME,
            DataTypes.LONG.getTypeSignature(),
            DataTypes.LONG.getTypeSignature()
        ),
        DataTypes.LONG,
        List.of(new InputColumn(0, DataTypes.LONG))
    );

    private final Aggregation countAggregation = new Aggregation(
        CountAggregation.SIGNATURE,
        CountAggregation.SIGNATURE.getReturnType().createType(),
        List.of(new InputColumn(0, ObjectType.UNTYPED))
    );

    @Before
    public void setup() {
        functions = createNodeContext().functions();
        RelationName name = new RelationName(DocSchemaInfo.NAME, "tbl");
        this.table = SQLExecutor.tableInfo(
            name,
            """
                create table tbl (
                    x bigint,
                    payload_subInt object as (col int not null)
                )
                """,
            clusterService);
        Map<RelationName, AnalyzedRelation> sources = Map.of(name, new TableRelation(this.table));
        e = new SqlExpressions(sources);
    }

    @Test
    public void test_create_aggregators_for_reference_and_doc_value_field_for_the_correct_field_type() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(e.asSymbol("tbl.x")),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, contains(instanceOf(SumAggregation.SumLong.class)));
    }

    @Test
    public void test_create_aggregators_for_cast_reference_returns_aggregator_only_if_it_is_cast_to_numeric() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(e.asSymbol("tbl.x::real")),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, is(nullValue()));

        aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(e.asSymbol("tbl.x::numeric")),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, contains(instanceOf(SumAggregation.SumLong.class)));
    }

    @Test
    public void test_create_aggregators_for_literal_aggregation_input_returns_null() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(Literal.of(1)),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, is(nullValue()));
    }

    @Test
    public void test_create_aggregators_for_reference_not_mapped_to_field_returns_null() {
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(Literal.of(1)),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, is(nullValue()));
    }

    @Test
    public void test_create_aggregators_for_reference_and_without_doc_value_field_returns_null() {
        Reference xRef = (Reference) e.asSymbol("tbl.x");
        var aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(xRef),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, is(Matchers.notNullValue()));

        aggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(longSumAggregation),
            List.of(new Reference(
                xRef.ident(),
                xRef.granularity(),
                xRef.valueType(),
                xRef.columnPolicy(),
                xRef.indexType(),
                xRef.isNullable(),
                false,
                xRef.position(),
                xRef.defaultExpression())
            ),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        assertThat(aggregators, is(Matchers.nullValue()));
    }

    @Test
    public void test_create_aggregators_for_multiple_aggregations() {
        var actualAggregators = DocValuesAggregates.createAggregators(
            functions,
            List.of(new Aggregation(
                        CountAggregation.SIGNATURE,
                        CountAggregation.SIGNATURE.getReturnType().createType(),
                        List.of(new InputColumn(0, ObjectType.UNTYPED))),
                    new Aggregation(
                        CountAggregation.SIGNATURE,
                        CountAggregation.SIGNATURE.getReturnType().createType(),
                        List.of(new InputColumn(1, ObjectType.UNTYPED))),
                    new Aggregation(
                        Signature.aggregate(
                            SumAggregation.NAME,
                            DataTypes.LONG.getTypeSignature(),
                            DataTypes.LONG.getTypeSignature()
                        ),
                        DataTypes.LONG,
                        List.of(new InputColumn(2, DataTypes.LONG)))

            ),
            List.of(e.asSymbol("tbl.Payload_subInt"), e.asSymbol("tbl.payload_subInt"),e.asSymbol("tbl.x")),
            SearchPath.pathWithPGCatalogAndDoc(),
            table
        );
        //select count(tbl.Payload_subInt), count(tbl.payload_subInt), sum(tbl.x) from tbl;
        assertThat(actualAggregators, containsInAnyOrder(instanceOf(SortedNumericDocValueAggregator.class),
                                                         instanceOf(SortedNumericDocValueAggregator.class),
                                                         instanceOf(SumAggregation.SumLong.class)));
    }
}
