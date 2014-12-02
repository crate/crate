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

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationVisitor;
import io.crate.metadata.*;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.operation.operator.OperatorModule;
import io.crate.operation.predicate.PredicateModule;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.assertLiteralSymbol;

public class ReferenceToTrueVisitorTest {

    private ReferenceToTrueVisitor visitor;
    private ExpressionAnalyzer expressionAnalyzer;
    private ExpressionAnalysisContext expressionAnalysisContext;

    @Before
    public void setUp() throws Exception {
        Injector injector = new ModulesBuilder()
            .add(new MetaDataModule())
            .add(new OperatorModule())
            .add(new MetaDataInformationModule())
            .add(new ScalarFunctionModule())
            .add(new PredicateModule()).createInjector();
        visitor = new ReferenceToTrueVisitor();
        expressionAnalyzer = new ExpressionAnalyzer(
                injector.getInstance(AnalysisMetaData.class),
                new ParameterContext(new Object[0], new Object[0][]),
                ImmutableMap.<QualifiedName, AnalyzedRelation>of(new QualifiedName("dummy"), new DummyRelation())
        );
        expressionAnalysisContext = new ExpressionAnalysisContext();
    }

    private Symbol convert(Symbol symbol) {
        return expressionAnalyzer.normalize(visitor.process(symbol, null));
    }

    /**
     * relation that will return a Reference with Doc granularity / String type for all columns
     */
    private static class DummyRelation implements AnalyzedRelation {

        @Override
        public <C, R> R accept(RelationVisitor<C, R> visitor, C context) {
            return null;
        }

        @Override
        public Field getField(ColumnIdent columnIdent) {
            return new Field(this, columnIdent.sqlFqn(),
                    new Reference(new ReferenceInfo(new ReferenceIdent(
                    new TableIdent("doc", "dummy"), columnIdent), RowGranularity.DOC, DataTypes.STRING)));
        }

        @Override
        public Field getWritableField(ColumnIdent path) throws UnsupportedOperationException {
            return null;
        }

        @Override
        public List<Field> fields() {
            return null;
        }
    }

    public Symbol fromSQL(String expression) {
        return expressionAnalyzer.convert(SqlParser.createExpression(expression), expressionAnalysisContext);
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

}
