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

package io.crate.expression.scalar.systeminformation;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.metadata.pgcatalog.OidHash;

public class PgGetFunctionResultFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_oid_results_in_null() {
        assertEvaluateNull("pg_function_is_visible(null)");
    }

    @Test
    public void test_function_retuns_null_for_values_that_are_not_oids() {
        assertEvaluateNull("pg_get_function_result(0)");
        assertEvaluateNull("pg_get_function_result(-14)");
    }

    @Test
    public void test_system_function_result_type_text_representation() {
        for (var signature : sqlExpressions.nodeCtx.functions().signatures()) {
            Integer funcOid = OidHash.functionOid(signature);
            assertEvaluate("pg_get_function_result(" + funcOid + ")",
                            signature.getReturnType().toString());
        }
    }
}
