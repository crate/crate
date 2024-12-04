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

package io.crate.expression.predicate;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.lucene.search.Query;
import org.junit.Test;

import io.crate.testing.IndexVersionCreated;

@IndexVersionCreated(value = 8_08_00_99) // V_5_8_0
public class FieldExistsQueryBeforeV590Test extends FieldExistsQueryTest {

    @Test
    public void testIsNullOnObjectArray() throws Exception {
        Query isNull = convert("o_array IS NULL");
        assertThat(isNull).hasToString("(o_array IS NULL)");
        Query isNotNull = convert("o_array IS NOT NULL");
        assertThat(isNotNull).hasToString("(NOT (o_array IS NULL))");
    }

    @Test
    public void test_neq_on_array() {
        Query query = convert("(y_array != [1])");
        // (+*:* -(y_array IS NULL)))~1) is required to make sure empty arrays are not filtered by the FieldExistsQuery
        assertThat(query).hasToString("+(+*:* -(+y_array:{1} +(y_array = [1::bigint]))) +((FieldExistsQuery [field=y_array] (+*:* -(y_array IS NULL)))~1)");
    }
}
