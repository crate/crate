/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.module.sql.test;

import org.cratedb.action.collect.scope.ExpressionScope;
import org.cratedb.action.collect.scope.GlobalExpressionDescription;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.stubs.HitchhikerMocks;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExpressionTest {

    protected ParsedStatement stmt;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected ParsedStatement execute(String stmt) throws Exception {
        return execute(stmt, new Object[0]);
    }

    protected ParsedStatement execute(String stmt, Object[] args) throws Exception {
        NodeExecutionContext nec = HitchhikerMocks.nodeExecutionContext();
        SQLParseService service = new SQLParseService(nec);
        this.stmt = service.parse(stmt, args);
        return this.stmt;
    }

    @Test
    public void testExpressionInResultColumnListGlobalAggregate() throws Exception {
        execute("select sys.cluster.name, count(distinct race) from characters");
        assertEquals("characters", stmt.tableName());
        assertEquals("sys.cluster.name", stmt.outputFields().get(0).v1());
        assertEquals("sys.cluster.name", stmt.outputFields().get(0).v2());
        assertEquals(2, stmt.resultColumnList().size());
        assertThat(stmt.resultColumnList().get(0), instanceOf(GlobalExpressionDescription.class));
        assertEquals(ExpressionScope.CLUSTER, ((GlobalExpressionDescription) stmt.resultColumnList().get(0)).scope());
        assertEquals("sys.cluster.name", ((GlobalExpressionDescription) stmt.resultColumnList().get(0)).name());
    }

    @Test
    public void testExpressionInResultColumnListGroupBy() throws Exception {
        execute("select sys.cluster.name from characters group by race, sys.cluster.name");
        assertEquals("characters", stmt.tableName());
        assertEquals("sys.cluster.name", stmt.outputFields().get(0).v1());
        assertEquals("sys.cluster.name", stmt.outputFields().get(0).v2());
        assertEquals(1, stmt.resultColumnList().size());
        assertThat(stmt.resultColumnList().get(0), instanceOf(GlobalExpressionDescription.class));
    }

    @Test
    public void testExpressionInResultColumnList() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Global expressions not allowed here.");
        execute("select sys.cluster.name from characters");
    }

    @Test
    public void errorOnGlobalexpressionNotInGroupByKey() throws Exception {
        expectedException.expect(SQLParseException.class);
        expectedException.expectMessage("Can only query columns that are listed in group by.");
        execute("select sys.cluster.name from characters group by race");
    }
}
