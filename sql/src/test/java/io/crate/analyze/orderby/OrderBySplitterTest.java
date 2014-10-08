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

package io.crate.analyze.orderby;

import io.crate.analyze.where.PartitionResolver;
import io.crate.metadata.*;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.relation.JoinRelation;
import io.crate.metadata.relation.TableRelation;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.operation.scalar.FormatFunction;
import io.crate.operation.scalar.SubstrFunction;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class OrderBySplitterTest {

    private OrderBySplitter splitter = new OrderBySplitter();
    private TableRelation relation1 = new TableRelation(TestingTableInfo.builder(
            new TableIdent(null, "t1"), RowGranularity.DOC, new Routing())
            .add(ColumnIdent.fromPath("id"), DataTypes.INTEGER)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build(), mock(PartitionResolver.class));
    private TableRelation relation2 = new TableRelation(TestingTableInfo.builder(
            new TableIdent(null, "t2"), RowGranularity.DOC, new Routing())
            .add(ColumnIdent.fromPath("id"), DataTypes.INTEGER)
            .addPrimaryKey("id")
            .add("name", DataTypes.STRING, null)
            .build(), mock(PartitionResolver.class));

    @Test
    public void testSplitOrderByReferences() throws Exception {
        Symbol idRefTable1 = TestingHelpers.createReference("t1", new ColumnIdent("id"), DataTypes.INTEGER);
        Symbol nameRefTable2 = TestingHelpers.createReference("t2", new ColumnIdent("name"), DataTypes.STRING);

        List<Symbol> sortSymbols = Arrays.asList(idRefTable1, nameRefTable2);
        Map<AnalyzedRelation, List<Symbol>> relationSymbolsMap = splitter.split(sortSymbols, relation1);
        assertThat(relationSymbolsMap.get(relation1), hasItem(TestingHelpers.isReference("id", DataTypes.INTEGER)));
        assertThat(relationSymbolsMap.get(relation1).size(), is(1));

        relationSymbolsMap = splitter.split(sortSymbols, relation2);
        assertThat(relationSymbolsMap.get(relation2), hasItem(TestingHelpers.isReference("name", DataTypes.STRING)));
        assertThat(relationSymbolsMap.get(relation2).size(), is(1));

        JoinRelation join = new JoinRelation(JoinRelation.Type.CROSS_JOIN, relation1, relation2);
        relationSymbolsMap = splitter.split(sortSymbols, join);
        assertThat(relationSymbolsMap.get(relation1), hasItem(TestingHelpers.isReference("id", DataTypes.INTEGER)));
        assertThat(relationSymbolsMap.get(relation1).size(), is(1));
        assertThat(relationSymbolsMap.get(relation2), hasItem(TestingHelpers.isReference("name", DataTypes.STRING)));
        assertThat(relationSymbolsMap.get(relation2).size(), is(1));
    }

    @Test
    public void testSplitOrderByInvalidReferences() throws Exception {
        Symbol invalidTable1 = TestingHelpers.createReference("t3", new ColumnIdent("foo"), DataTypes.INTEGER);
        Symbol invalidTable2 = TestingHelpers.createReference("t4", new ColumnIdent("bar"), DataTypes.STRING);

        List<Symbol> sortSymbols = Arrays.asList(invalidTable1, invalidTable2);
        JoinRelation join = new JoinRelation(JoinRelation.Type.CROSS_JOIN, relation1, relation2);
        Map<AnalyzedRelation, List<Symbol>> relationSymbolsMap = splitter.split(sortSymbols, join);
        assertThat(relationSymbolsMap.get(join).size(), is(0));
        assertThat(relationSymbolsMap.get(relation1).size(), is(0));
        assertThat(relationSymbolsMap.get(relation2).size(), is(0));
    }

    @Test
    public void testSplitOrderByLiterals() throws Exception {
        Symbol integerLiteralSymbol = Literal.newLiteral(1);
        Symbol stringLiteralSymbol = Literal.newLiteral("name");
        Symbol booleanLiteralSymbol = Literal.newLiteral(true);

        List<Symbol> sortSymbols = Arrays.asList(integerLiteralSymbol, stringLiteralSymbol, booleanLiteralSymbol);
        JoinRelation join = new JoinRelation(JoinRelation.Type.CROSS_JOIN, relation1, relation2);
        Map<AnalyzedRelation, List<Symbol>> relationSymbolsMap = splitter.split(sortSymbols, join);
        assertThat(relationSymbolsMap.get(relation1).size(), is(0));
        assertThat(relationSymbolsMap.get(relation2).size(), is(0));
    }

    @Test
    public void testSplitOrderByFunctions() throws Exception {
        Symbol idRefTable1 = TestingHelpers.createReference("t1", new ColumnIdent("id"), DataTypes.INTEGER);
        Symbol addFunctionSymbol = TestingHelpers.createFunction(
                AddFunction.NAME,
                DataTypes.INTEGER,
                Arrays.asList(idRefTable1, Literal.newLiteral(2))
        );

        Symbol nameRefTable2 = TestingHelpers.createReference("t2", new ColumnIdent("name"), DataTypes.STRING);
        Symbol substrFunctionSymbol = TestingHelpers.createFunction(
                SubstrFunction.NAME,
                DataTypes.STRING,
                Arrays.asList(nameRefTable2, Literal.newLiteral(0), Literal.newLiteral(3))
        );

        Symbol nameRefTable1 = TestingHelpers.createReference("t1", new ColumnIdent("name"), DataTypes.STRING);
        Symbol strFormatFunctionSymbol = TestingHelpers.createFunction(
                FormatFunction.NAME,
                DataTypes.STRING,
                Arrays.asList(Literal.newLiteral("%s %s"), nameRefTable1, nameRefTable2)
        );

        List<Symbol> sortSymbols = Arrays.asList(addFunctionSymbol, substrFunctionSymbol, strFormatFunctionSymbol);
        JoinRelation join = new JoinRelation(JoinRelation.Type.CROSS_JOIN, relation1, relation2);
        Map<AnalyzedRelation, List<Symbol>> relationSymbolsMap = splitter.split(sortSymbols, join);
        assertThat(relationSymbolsMap.get(relation1), hasItem(TestingHelpers.isFunction(AddFunction.NAME)));
        assertThat(relationSymbolsMap.get(relation1).size(), is(1));
        assertThat(relationSymbolsMap.get(relation2), hasItem(TestingHelpers.isFunction(SubstrFunction.NAME)));
        assertThat(relationSymbolsMap.get(relation2).size(), is(1));
        assertThat(relationSymbolsMap.get(join), hasItem(TestingHelpers.isFunction(FormatFunction.NAME)));
        assertThat(relationSymbolsMap.get(join).size(), is(1));
    }
}

