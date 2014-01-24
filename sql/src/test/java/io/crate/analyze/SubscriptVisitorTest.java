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

package io.crate.analyze;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class SubscriptVisitorTest {

    public SubscriptVisitor visitor = new SubscriptVisitor();

    @Test
    public void testVisitSubscriptExpression() throws Exception {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression("a['x']['y']");
        expression.accept(visitor, context);

        assertEquals("a", context.column());
        assertEquals("x", context.parts().get(0));
        assertEquals("y", context.parts().get(1));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidSubscriptExpressionName() throws Exception {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression("'a'['x']['y']");
        expression.accept(visitor, context);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidSubscriptExpressionIndex() throws Exception {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression("a[x]['y']");
        expression.accept(visitor, context);
    }
}
