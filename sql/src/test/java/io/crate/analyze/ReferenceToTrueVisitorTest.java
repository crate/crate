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
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.isLiteral;


public class ReferenceToTrueVisitorTest extends CrateUnitTest {

    private ReferenceToTrueVisitor visitor;
    private SqlExpressions expressions;

    @Before
    public void prepare() throws Exception {
        visitor = new ReferenceToTrueVisitor();
        ImmutableMap<QualifiedName, AnalyzedRelation> sources = ImmutableMap.<QualifiedName, AnalyzedRelation>of(
                new QualifiedName("dummy"), new DummyRelation());
        expressions = new SqlExpressions(sources);
    }

    private Symbol convert(Symbol symbol) {
        return expressions.normalize(visitor.process(symbol, null));
    }

    /**
     * relation that will return a Reference with Doc granularity / String type for all columns
     */
    private static class DummyRelation implements AnalyzedRelation {

        @Override
        public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
            return null;
        }

        @Override
        public Field getField(Path path) {
            return new Field(this, path, DataTypes.STRING);
        }

        @Override
        public Field getWritableField(Path path) throws UnsupportedOperationException {
            return null;
        }

        @Override
        public List<Field> fields() {
            return null;
        }
    }

    public Symbol fromSQL(String expression) {
        return expressions.asSymbol(expression);
    }

    @Test
    public void testFalseAndMatchFunction() throws Exception {
        Symbol symbol = convert(fromSQL("false and match (table_name, 'jalla')"));
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testTrueAndMatchFunction() throws Exception {
        Symbol symbol = convert(fromSQL("true and match (table_name, 'jalla')"));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testComplexNestedDifferentMethods() throws Exception {
        Symbol symbol = convert(fromSQL(
            "number_of_shards = 1 or (number_of_replicas = 3 and schema_name = 'sys') " +
                "or not (number_of_shards = 2) and substr(table_name, 1, 1) = '1'"));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testIsNull() throws Exception {
        Symbol symbol = convert(fromSQL("clustered_by is null"));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testNot_NullAndSubstr() throws Exception {
        Symbol symbol = convert(fromSQL("not (null and substr(table_name, 1, 1) = '1')"));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testNot_FalseAndSubstr() throws Exception {
        Symbol symbol = convert(fromSQL("not (false and substr(table_name, 1, 1) = '1')"));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testNotPredicate() throws Exception {
        Symbol symbol = convert(fromSQL(("not (clustered_by = 'foo')")));
        assertThat(symbol, isLiteral(true));
    }

    @Test
    public void testComplexNestedDifferentMethodsEvaluatesToFalse() throws Exception {
        Symbol symbol = convert(fromSQL(
            "(number_of_shards = 1 or number_of_replicas = 3 and schema_name = 'sys' " +
                "or not (number_of_shards = 2)) and substr(table_name, 1, 1) = '1' and false"));
        assertThat(symbol, isLiteral(false));
    }

    @Test
    public void testNullAndMatchFunction() throws Exception {
        Symbol symbol = convert(fromSQL("null and match (table_name, 'jalla')"));
        assertThat(symbol, isLiteral(null, DataTypes.BOOLEAN));
    }

}
