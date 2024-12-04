/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.scalar;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.FloatVectorType;

public class VectorSimilarityFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_arg_returns_null() {
        assertEvaluateNull("vector_similarity(null, [1.2]::float_vector(1))");
        assertEvaluateNull("vector_similarity([1.2]::float_vector(1), null)");
    }

    @Test
    public void test_returns_similarity_based_on_euclidean_distance() {
        float[] v1 = new float[]{1.2f, 1.3f, 1.4f};
        float[] v2 = new float[]{2.2f, 2.3f, 2.4f};
        FloatVectorType vectorType = new FloatVectorType(3);
        float expected = org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN.compare(v1, v2);
        assertEvaluate("vector_similarity(?, ?)", expected, Literal.of(vectorType, v1), Literal.of(vectorType, v2));
    }

    @Test
    public void test_returns_max_similarity_one_for_coinciding_vectors() {
        assertEvaluate("vector_similarity([1.2, 1.3]::float_vector(2), [1.2, 1.3]::float_vector(2))", 1.0f);
    }

    @Test
    public void test_vectors_must_have_the_same_length() {
        assertThatThrownBy(() -> assertEvaluate("vector_similarity([1.2, 1.3]::float_vector(2), [1.2]::float_vector(2))", 1.0f))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Vectors must have same length");

    }
}
