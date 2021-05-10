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

package io.crate.expression;

import io.crate.metadata.ColumnIdent;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ValueExtractorsTest {

    @Test
    public void testIntegerAreConvertedToMatchType() {
        Function<Map<String, Object>, Object> extractValue =
            ValueExtractors.fromMap(new ColumnIdent("name"), DataTypes.LONG);

        // In xContent/JSON the type is not preserved, so numeric types may use a smaller type (e.g. int instead of long)
        // In order to have functions operate correctly we need to restore the correct type
        assertThat(
            extractValue.apply(Collections.singletonMap("name", 10)),
            is(10L)
        );
    }
}
