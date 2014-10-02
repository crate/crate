/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.PartitionName;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.testing.QueryPrinter;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

public class PartitionVisitorTest {

    private PartitionVisitor partitionVisitor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final List<String> PARTITIONS = ImmutableList.of(
            new PartitionName("parted", ImmutableList.of(new BytesRef("0"))).stringValue(),
            new PartitionName("parted", ImmutableList.of(new BytesRef("1"))).stringValue()
    );
    private static final TableInfo TABLE_INFO = new TestingTableInfo.Builder(
            new TableIdent(DocSchemaInfo.NAME, "parted"),
            RowGranularity.DOC,
            new Routing(
                    ImmutableMap.<String, Map<String, Set<Integer>>>of(
                            "foo", ImmutableMap.<String, Set<Integer>>of(
                                    "parted", ImmutableSet.of(1, 2, 3)))))
            .add("id", DataTypes.INTEGER, null)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .add("ts", DataTypes.TIMESTAMP, null, ReferenceInfo.ObjectType.DYNAMIC, ReferenceInfo.IndexType.NOT_ANALYZED, true)
            .addPrimaryKey("ts")
            .addPartitions(
                PARTITIONS.get(0),
                PARTITIONS.get(1)
            )
            .build();

    private static final TableInfo NO_PARTED_INFO = new TestingTableInfo.Builder(
            new TableIdent(DocSchemaInfo.NAME, "no_parted"),
            RowGranularity.DOC,
            new Routing(
                    ImmutableMap.<String, Map<String, Set<Integer>>>of(
                            "foo", ImmutableMap.<String, Set<Integer>>of(
                                    "no_parted", ImmutableSet.of(1, 2, 3)))))
            .add("id", DataTypes.INTEGER, null)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build();

    @Before
    public void before() {
        Injector injector = new ModulesBuilder()
                .add(new ScalarFunctionModule())
                .add(new OperatorModule())
                .add(new PredicateModule())
                .add(new MetaDataModule())
                .createInjector();
        Functions functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = injector.getInstance(ReferenceResolver.class);
        this.partitionVisitor = new PartitionVisitor(
                new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver));
    }

    @Test
    public void testMatchAll() throws Exception {
        PartitionVisitor.Context ctx = partitionVisitor.process(WhereClause.MATCH_ALL, TABLE_INFO);
        assertThat(ctx.whereClause(), is(WhereClause.MATCH_ALL));
        assertThat(ctx.partitions(), is(PARTITIONS));
    }

    @Test
    public void testNoMatch() throws Exception {
        PartitionVisitor.Context ctx = partitionVisitor.process(WhereClause.NO_MATCH, TABLE_INFO);
        assertThat(ctx.whereClause(), is(WhereClause.NO_MATCH));
        assertThat(ctx.partitions().size(), is(0));
    }

    @Test
    public void testNoPartitionedColumn() throws Exception {
        Function query = and(
                eq(ref(TABLE_INFO.ident(), "name", DataTypes.STRING), Literal.EMPTY_STRING),
                not(gt(ref(TABLE_INFO.ident(), "id", DataTypes.INTEGER), Literal.newLiteral(0)))
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions(), is(PARTITIONS)); // all partitions affected
        assertThat(ctx.whereClause(), is(whereClause)); // no change
    }

    @Test
    public void testNoPartitionedTable() throws Exception {
        Function query = and(
                eq(
                        ref(NO_PARTED_INFO.ident(), "name", DataTypes.STRING, RowGranularity.DOC),
                        Literal.EMPTY_STRING
                ),
                not(
                        gt(
                                ref(NO_PARTED_INFO.ident(), "id", DataTypes.INTEGER, RowGranularity.DOC),
                                Literal.newLiteral(0)
                        )
                )
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, NO_PARTED_INFO);
        assertThat(ctx.partitions().size(), is(0));
        assertThat(ctx.whereClause(), is(whereClause)); // no change
    }

    @Test
    public void testEqPartitionedColumnExistingPartition() throws Exception {
        Function query = eq(
                ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions().size(), is(1));
        assertThat(ctx.partitions().get(0), is(PARTITIONS.get(0)));
        assertThat(ctx.whereClause().noMatch(), is(false));
        assertThat(ctx.whereClause().hasQuery(), is(false));
    }

    @Test
    public void testEqPartitionedColumnNonExistingPartition() throws Exception {
        Function query = eq(
                ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                Literal.newLiteral(DataTypes.TIMESTAMP, 3L)
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions().size(), is(0));
        assertThat(ctx.whereClause().noMatch(), is(true));
    }

    @Test
    public void testNonExistingPartitionAnd() throws Exception {
        Function query = and(
                gt(
                        ref(TABLE_INFO.ident(), "id", DataTypes.INTEGER),
                        Literal.newLiteral(1)
                        ),
                eq(
                    ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                    Literal.newLiteral(DataTypes.TIMESTAMP, 3L)
            )
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions().size(), is(0));
        assertThat(ctx.whereClause().noMatch(), is(true));
    }

    @Test
    public void testNonExistingPartitionOr() throws Exception {
        Function query = or(
                gt(
                        ref(TABLE_INFO.ident(), "id", DataTypes.INTEGER),
                        Literal.newLiteral(1)
                ),
                eq(
                        ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 3L)
                )
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions(), is(PARTITIONS));
        assertThat(QueryPrinter.print(ctx.whereClause().query()), is("op_or(op_>(parted.id, 1), false)"));
    }

    @Test
    public void testExistingPartitionOr() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Using a partitioned column and a " +
                "normal column inside an OR clause is not supported");

        Function query = or(
                gt(
                        ref(TABLE_INFO.ident(), "id", DataTypes.INTEGER),
                        Literal.newLiteral(1)
                ),
                eq(
                        ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 1L)
                )
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions(), is(PARTITIONS));
        assertThat(QueryPrinter.print(ctx.whereClause().query()), is("op_or(op_>(parted.id, 1), false)"));
    }

    @Test
    public void testNotEqualsExistingPartition() throws Exception {
        Function query = not(
                eq(
                        ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 2L)
                )
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions(), hasItems(PARTITIONS.get(0)));
        assertThat(ctx.whereClause().noMatch(), is(false));
        assertThat(ctx.whereClause().hasQuery(), is(false));
    }

    @Test
    public void testAnd() throws Exception {
        Function query = and(
                eq(
                        ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 0L)
                ),
                eq(
                        ref(TABLE_INFO.ident(), "ts", DataTypes.TIMESTAMP, RowGranularity.SHARD),
                        Literal.newLiteral(DataTypes.TIMESTAMP, 1L)
                )
        );
        WhereClause whereClause = new WhereClause(query);
        PartitionVisitor.Context ctx = partitionVisitor.process(whereClause, TABLE_INFO);
        assertThat(ctx.partitions().size(), is(0));
        assertThat(ctx.whereClause().noMatch(), is(true));
    }
}
