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

package io.crate.expression.scalar.arithmetic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Function;
import io.crate.metadata.SearchPath;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;


public class RandomFunctionTest extends ScalarTestCase {

    private RandomFunction random;
    private TransactionContext txnCtx = TransactionContext.of(DUMMY_SESSION_INFO);

    @Before
    public void prepareRandom() {
        random = (RandomFunction) sqlExpressions.nodeCtx.functions().get(
            null, RandomFunction.NAME, Collections.emptyList(), SearchPath.pathWithPGCatalogAndDoc());
    }

    @Test
    public void testEvaluateRandom() {
        assertThat(random.evaluate(txnCtx, sqlExpressions.nodeCtx)).isBetween(0.0, 1.0);
    }

    @Test
    public void normalizeReference() {
        Function function = new Function(random.signature(), Collections.emptyList(), DataTypes.DOUBLE);
        Function normalized = (Function) random.normalizeSymbol(function, txnCtx, sqlExpressions.nodeCtx);
        assertThat(normalized, sameInstance(function));
    }

}
