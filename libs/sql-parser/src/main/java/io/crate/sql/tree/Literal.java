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

package io.crate.sql.tree;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class Literal
    extends Expression {

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    public static Literal fromObject(Object value) {
        return switch (value) {
            case null -> NullLiteral.INSTANCE;
            case BigDecimal val -> new NumericLiteral(val);
            case Double val -> new DoubleLiteral(val);
            case Float val -> new DoubleLiteral(val.doubleValue());
            case Short val -> new IntegerLiteral(val.intValue());
            case Integer val -> new IntegerLiteral(val.intValue());
            case Long val -> new LongLiteral(val);
            case Boolean val -> val ? BooleanLiteral.TRUE_LITERAL : BooleanLiteral.FALSE_LITERAL;
            case Object[] arr -> {
                ArrayList<Expression> expressions = new ArrayList<>(arr.length);
                for (Object o : arr) {
                    expressions.add(fromObject(o));
                }
                yield new ArrayLiteral(expressions);
            }
            case Map<?, ?> m -> {
                @SuppressWarnings("unchecked") Map<String, Object> valueMap = (Map<String, Object>) m;
                HashMap<String, Expression> map = HashMap.newHashMap(valueMap.size());
                for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
                    map.put(entry.getKey(), fromObject(entry.getValue()));
                }
                yield new ObjectLiteral(map);
            }
            default -> new StringLiteral(value.toString());
        };
    }
}
