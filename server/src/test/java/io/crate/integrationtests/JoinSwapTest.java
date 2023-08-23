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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.execution.engine.join.RamBlockSizeCalculator;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.metadata.RelationName;
import io.crate.statistics.Stats;
import io.crate.statistics.TableStats;
import io.crate.testing.Asserts;
import io.crate.testing.UseHashJoins;
import io.crate.testing.UseJdbc;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;
import io.crate.types.DataTypes;

@IntegTestCase.ClusterScope(minNumDataNodes = 2)
public class JoinSwapTest extends IntegTestCase {

    /*
     * https://github.com/crate/crate/issues/14583
     */
    @UseJdbc(0)
    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(value = 0)
    @UseHashJoins(1)
    public void test_hash_join() throws Exception {
        execute("create table doc.t1(a int, b int)");
        execute("create table doc.t2(c int, d int)");
        execute("create table doc.t3(e int, f int)");

        execute("insert into doc.t1(a,b) values(1,2)");
        execute("insert into doc.t2(c,d) values (1,3),(5,6)");
        execute("insert into doc.t3(e,f) values (3,2)");

        execute("refresh table doc.t1, doc.t2, doc.t3");
        execute("analyze");

        var stmt = "SELECT t3.e FROM t1 JOIN t3 ON t1.b = t3.f JOIN t2 ON t1.a = t2.c AND t2.d = t3.e";
        System.out.println(stmt);
        execute("explain " + stmt);
        System.out.println(response.rows()[0][0]);
        execute(stmt);
        assertThat(response).hasRows("3");

    }


    /*
     * https://github.com/crate/crate/issues/14583
     */
    @UseJdbc(0)
    @Test
    @UseRandomizedSchema(random = false)
    @UseRandomizedOptimizerRules(value = 0)
    @UseHashJoins(1)
    public void test_hash_join_reordering_does_work() throws Exception {
        execute("create table doc.t1(a int, b int)");
        execute("create table doc.t2(c int, d int)");
        execute("create table doc.t3(e int, f int)");

        execute("insert into doc.t1(a,b) values(1,2)");
        execute("insert into doc.t2(c,d) values (1,3),(5,6)");
        execute("insert into doc.t3(e,f) values (3,2)");

        execute("refresh table doc.t1, doc.t2, doc.t3");
        execute("analyze");

        System.out.println("---------------------------------------------------");
        var stmt = "SELECT t3.e FROM t1 JOIN t3 ON t1.b = t3.f JOIN t2 ON t2.d = t3.e AND t1.a = t2.c";
        System.out.println(stmt);
        execute("explain " + stmt);
        System.out.println(response.rows()[0][0]);
        execute(stmt);
        assertThat(response).hasRows("3");
    }

}
