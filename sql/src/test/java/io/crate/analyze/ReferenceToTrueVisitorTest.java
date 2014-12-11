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
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;

public class ReferenceToTrueVisitorTest {

    private ReferenceToTrueVisitor visitor;
    private EvaluatingNormalizer normalizer;
    private SelectAnalyzedStatement selectAnalysis;
    private SelectStatementAnalyzer analyzer;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
            .add(new BaseAnalyzerTest.TestModule())
            .add(new MetaDataModule())
            .add(new OperatorModule())
            .add(new MetaDataInformationModule())
            .add(new ScalarFunctionModule())
            .add(new PredicateModule()).createInjector();
        visitor = new ReferenceToTrueVisitor();
        Functions functions = injector.getInstance(Functions.class);
        ReferenceResolver referenceResolver = injector.getInstance(ReferenceResolver.class);
        ReferenceInfos referenceInfos = injector.getInstance(ReferenceInfos.class);
        normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
        analyzer = new SelectStatementAnalyzer(
            referenceInfos,
            functions,
            referenceResolver
        );
        selectAnalysis = (SelectAnalyzedStatement) analyzer.newAnalysis(
            new ParameterContext(new Object[0], new Object[0][]));
    }

    public Symbol fromSQL(String expression) {
        analyzer.process(new Table(new QualifiedName(ImmutableList.of("information_schema", "tables"))), selectAnalysis);
        return analyzer.process(SqlParser.createExpression(expression), selectAnalysis);
    }

    @Test
    public void testFalseAndMatchFunction() throws Exception {
        Symbol symbol = convert(fromSQL("false and match (table_name, 'jalla')"));
        assertLiteralSymbol(symbol, false);
    }

    @Test
    public void testTrueAndMatchFunction() throws Exception {
        Symbol symbol = convert(fromSQL("true and match (table_name, 'jalla')"));
        assertLiteralSymbol(symbol, true);
    }

    @Test
    public void testComplexNestedDifferentMethods() throws Exception {
        Symbol symbol = convert(fromSQL(
            "number_of_shards = 1 or (number_of_replicas = 3 and schema_name = 'sys') " +
                "or not (number_of_shards = 2) and substr(table_name, 1, 1) = '1'"));
        assertLiteralSymbol(symbol, true);
    }

    @Test
    public void testIsNull() throws Exception {
        Symbol symbol = convert(fromSQL("clustered_by is null"));
        assertLiteralSymbol(symbol, true);
    }

    @Test
    public void testNot_NullAndSubstr() throws Exception {
        Symbol symbol = convert(fromSQL("not (null and substr(table_name, 1, 1) = '1')"));
        assertLiteralSymbol(symbol, null, DataTypes.BOOLEAN);
    }

    @Test
    public void testNot_FalseAndSubstr() throws Exception {
        Symbol symbol = convert(fromSQL("not (false and substr(table_name, 1, 1) = '1')"));
        assertLiteralSymbol(symbol, true);
    }

    @Test
    public void testNotPredicate() throws Exception {
        Symbol symbol = convert(fromSQL(("not (clustered_by = 'foo')")));
        assertLiteralSymbol(symbol, true);
    }

    @Test
    public void testComplexNestedDifferentMethodsEvaluatesToFalse() throws Exception {
        Symbol symbol = convert(fromSQL(
            "(number_of_shards = 1 or number_of_replicas = 3 and schema_name = 'sys' " +
                "or not (number_of_shards = 2)) and substr(table_name, 1, 1) = '1' and false"));
        assertLiteralSymbol(symbol, false);
    }

    @Test
    public void testNullAndMatchFunction() throws Exception {
        Symbol symbol = convert(fromSQL("null and match (table_name, 'jalla')"));
        assertLiteralSymbol(symbol, null, DataTypes.BOOLEAN);
    }

    public Symbol convert(Symbol symbol) {
        return normalizer.normalize(visitor.process(symbol, null));
    }
}
