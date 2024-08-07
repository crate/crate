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

import java.util.List;

import org.junit.Test;

import io.crate.expression.operator.DistinctFromPredicate;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

/*
A <distinct predicate> tests two values to see whether they are distinct and
returns either ``TRUE`` or ``FALSE``.
 */
public class DistinctFromPredicateTest extends ScalarTestCase {

    // * The two expressions must be comparable. If the comparands are rows, they
    //  must be of the same degree and each corresponding pair of Fields must have
    //  comparable <data type>s.
    //
    @Test(expected = ClassCastException.class)
    public void testEvaluateIncomparableDatatypes() {
        assertEvaluate("3 is distinct from true", isLiteral(false));
    }

    //* The <distinct predicate> can't be used with ``BLOB``\s, ``CLOB``\s or
    //  ``NCLOB``\s.


    @Test
    public void testNormalizeSymbolNullTrue() {
        // The <distinct predicate> is ``TRUE`` if the value of ``expression_1`` is not
        //equal to the value of ``expression_2`` -- that is, ``IS DISTINCT FROM`` is the
        //same as the ``<>`` <comparison predicate> except both null.
        // A ``NULL`` value is distinct from every non-null value.
        assertNormalize("'a' is distinct from null", isLiteral(Boolean.TRUE));
        assertNormalize("2 is distinct from null", isLiteral(Boolean.TRUE));
        assertNormalize("null is distinct from 'a'", isLiteral(Boolean.TRUE));
    }

    @Test
    public void testNormalizeSymbolNullNull() {
        // two ``NULL`` values are not distinct from one other
        assertNormalize("null is distinct from null", isLiteral(Boolean.FALSE));
    }

    @Test
    public void testNormalizeReference() {
        assertNormalize("name is distinct from age", isFunction(DistinctFromPredicate.NAME));
    }

    @Test
    public void testNormalizeUndefined() {
        assertNormalize("obj_ignored['column'] is distinct from name", isFunction(DistinctFromPredicate.NAME));
    }

    @Test
    public void testNormalizeUndefinedNull() {
        assertNormalize("obj_ignored['column'] is distinct from null", isFunction(DistinctFromPredicate.NAME));
    }

    @Test
    public void testEvaluateDistinctStrings() {
        assertEvaluate("name is distinct from 'two'", true, Literal.of(DataTypes.STRING, "one"));
    }

    @Test
    public void testEvaluateNonDistinctStrings() {
        assertEvaluate("name is distinct from 'two'", false, Literal.of(DataTypes.STRING, "two"));
    }

    @Test
    public void testEvaluateDistinctNumbers() {
        assertEvaluate("id is distinct from 2", true, Literal.of(DataTypes.INTEGER, 1));
    }

    @Test
    public void testEvaluateNonDistinctNumbers() {
        assertEvaluate("id is distinct from 2", false, Literal.of(DataTypes.INTEGER, 2));
    }

    @Test
    public void testEvaluateStringNull() {
        assertEvaluate("name is distinct from null", true, Literal.of(DataTypes.STRING, "one"));
    }

    @Test
    public void testEvaluateNulls() {
        assertEvaluate("null is distinct from null", false);
    }

    @Test
    public void testEvaluateWithInputThatReturnsNull() {
        assertEvaluate("name is distinct from null", false, Literal.of(DataTypes.STRING, null));
    }

    //
    // NOT DISTINCT cases
    //


    // * The two expressions must be comparable. If the comparands are rows, they
    //  must be of the same degree and each corresponding pair of Fields must have
    //  comparable <data type>s.
    //
    @Test(expected = java.lang.ClassCastException.class)
    public void testNotEvaluateIncomparableDatatypes() {
        assertEvaluate("3 is not distinct from false", isLiteral(false));
    }

    //* The <distinct predicate> can't be used with ``BLOB``\s, ``CLOB``\s or
    //  ``NCLOB``\s.


    @Test
    public void testNotNormalizeSymbolTrue() {
        // The <distinct predicate> is ``TRUE`` if the value of ``expression_1`` is not
        //equal to the value of ``expression_2`` -- that is, ``IS DISTINCT FROM`` is the
        //same as the ``<>`` <comparison predicate> except both null.
        // A ``NULL`` value is distinct from every non-null value.
        assertNormalize("'a' is not distinct from null", isLiteral(false));
        assertNormalize("null is not distinct from 'a'", isLiteral(false));
    }

    @Test
    public void testNotNormalizeSymbolFalse() {
        // two ``NULL`` values are not distinct from one other
        assertNormalize("null is not distinct from null", isLiteral(true));
    }

    @Test
    public void testNotNormalizeReference() {
        assertNormalize("name is not distinct from age", isFunction(NotPredicate.NAME));
    }

    @Test
    public void testNotNormalizeUndefined() {
        assertNormalize("obj_ignored['column'] is not distinct from name", isFunction(NotPredicate.NAME, List.of(DataTypes.BOOLEAN)));
    }

    @Test
    public void testNotNormalizeUndefinedNull() {
        assertNormalize("obj_ignored['column'] is not distinct from null", isFunction(NotPredicate.NAME, List.of(DataTypes.BOOLEAN)));
    }

    @Test
    public void testNotEvaluateDistinctStrings() {
        assertEvaluate("name is not distinct from 'two'", false, Literal.of(DataTypes.STRING, "one"));
    }

    @Test
    public void testNotEvaluateNonDistinctStrings() {
        assertEvaluate("name is not distinct from 'two'", true, Literal.of(DataTypes.STRING, "two"));
    }

    @Test
    public void testNotEvaluateDistinctNumbers() {
        assertEvaluate("id is not distinct from 2", false, Literal.of(DataTypes.INTEGER, 1));
    }

    @Test
    public void testNotEvaluateNonDistinctNumbers() {
        assertEvaluate("id is not distinct from 2", true, Literal.of(DataTypes.INTEGER, 2));
    }

    @Test
    public void testNotEvaluateStringNull() {
        assertEvaluate("name is not distinct from null", false, Literal.of(DataTypes.STRING, "one"));
    }

    @Test
    public void testNotEvaluateNulls() {
        assertEvaluate("null is not distinct from null", true);
    }

    @Test
    public void testNotEvaluateWithInputThatReturnsNull() {
        assertEvaluate("name is not distinct from null", true, Literal.of(DataTypes.STRING, null));
    }



    // Array comparison

    @Test
    public void testDistinctArrayLeftSideIsNull_RightSideNull() throws Exception {
        assertEvaluate("[1, 10] IS DISTINCT FROM null", true);
        assertEvaluate("null IS DISTINCT FROM [1, 10]", true);
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsTrueIfDistinct() throws Exception {
        assertNormalize("[ [1, 1], [10] ] IS DISTINCT FROM [ [1, 1], [10] ]", isLiteral(false));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsFalseIfNotDistinct() throws Exception {
        assertNormalize("[ [1, 1], [10] ] IS DISTINCT FROM [ [1], [10] ]", isLiteral(true));
    }

    @Test
    public void testNormalizeAndEvalTwoDistinctArraysShouldReturnTrueLiteral() throws Exception {
        assertNormalize("[1, 1, 10] IS DISTINCT FROM [1, 1, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoNotDistinctArraysShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] IS DISTINCT FROM [1, 10]", isLiteral(true));
    }

    @Test
    public void testNormalizeAndEvalTwoArraysWithSameLengthButDifferentValuesShouldBeDistinct() throws Exception {
        assertNormalize("[1, 1, 10] IS DISTINCT FROM [1, 2, 10]", isLiteral(true));
    }

    // Array comparison with values
    @Test
    public void testDistinctArrayLeftSideIsNull_RightSideNullWithValue() throws Exception {
        ArrayType<Integer> intTypeArr = new ArrayType<>(DataTypes.INTEGER);
        assertEvaluate("int_array IS DISTINCT FROM null", true, Literal.of(intTypeArr, List.of(1, 10)));
        assertEvaluate("null IS DISTINCT FROM int_array", true, Literal.of(intTypeArr, List.of(1, 10)));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsTrueIfDistinctWithValue() throws Exception {
        assertNormalize("[ [1, 1], [10] ] IS DISTINCT FROM [ [1, 1], [10] ]", isLiteral(false));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsFalseIfNotDistinctWithValue() throws Exception {
        assertNormalize("[ [1, 1], [10] ] IS DISTINCT FROM [ [1], [10] ]", isLiteral(true));
        ArrayType<Integer> intTypeArr = new ArrayType<>(DataTypes.INTEGER);
        assertEvaluate("int_array IS DISTINCT FROM int_array", true, Literal.of(intTypeArr, List.of(1, 10)), Literal.of(intTypeArr, List.of(2, 20)));
        assertEvaluate("int_array IS NOT DISTINCT FROM int_array", true, Literal.of(intTypeArr, List.of(1, 10)), Literal.of(intTypeArr, List.of(1, 10)));
    }

    @Test
    public void testNormalizeAndEvalTwoDistinctArraysShouldReturnTrueLiteralWithValue() throws Exception {
        assertNormalize("[1, 1, 10] IS DISTINCT FROM [1, 1, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoNotDistinctArraysShouldReturnFalseWithValue() throws Exception {
        assertNormalize("[1, 1, 10] IS DISTINCT FROM [1, 10]", isLiteral(true));
    }

    @Test
    public void testNormalizeAndEvalTwoArraysWithSameLengthButDifferentValuesShouldBeDistinctWithValue() throws Exception {
        assertNormalize("[1, 1, 10] IS DISTINCT FROM [1, 2, 10]", isLiteral(true));
    }


}
