/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.ValidationException;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Test;

public class RelationAnalyzerTest extends CrateUnitTest {

    private SQLExecutor executor = SQLExecutor.builder(new NoopClusterService()).enableDefaultTables().build();

    @Test
    public void testValidateUsedRelationsInJoinConditions() throws Exception {
        expectedException.expect(ValidationException.class);
        expectedException.expectMessage("missing FROM-clause entry for relation 't3'");
        executor.analyze("select * from t1 join t2 on t1.a = t3.c join t3 on t2.b = t3.c");
    }

    @Test
    public void testMaxNumberOfRelations() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (short i = 0; i <= StatementAnalysisContext.MAX_RELATIONS; i++) {
            sb.append("select * from (");
        }
        sb.append("select * from t1");
        for (short i = 0; i <= StatementAnalysisContext.MAX_RELATIONS; i++) {
            sb.append(") a").append(i);
        }
        System.out.println(sb.toString());
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Query with more than " + StatementAnalysisContext.MAX_RELATIONS + " relations");
        executor.analyze(sb.toString());
    }
}
