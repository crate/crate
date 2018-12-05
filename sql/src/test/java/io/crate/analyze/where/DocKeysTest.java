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

package io.crate.analyze.where;

import com.google.common.collect.ImmutableList;
import io.crate.data.Row;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.TestingHelpers.getFunctions;
import static org.hamcrest.core.Is.is;

public class DocKeysTest extends CrateUnitTest {


    @Test
    public void testClusteredIsFirstInId() throws Exception {
        // if a the table is clustered and has a pk, the clustering value is put in front in the id computation
        List<List<Symbol>> pks = ImmutableList.<List<Symbol>>of(
            ImmutableList.<Symbol>of(Literal.of(1), Literal.of("Ford"))
        );
        DocKeys docKeys = new DocKeys(pks, false, 1, null);
        DocKeys.DocKey key = docKeys.getOnlyKey();
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        assertThat(key.getRouting(txnCtx, getFunctions(), Row.EMPTY, SubQueryResults.EMPTY), is("Ford"));
        assertThat(key.getId(txnCtx, getFunctions(), Row.EMPTY, SubQueryResults.EMPTY), is("AgRGb3JkATE="));
    }

}
