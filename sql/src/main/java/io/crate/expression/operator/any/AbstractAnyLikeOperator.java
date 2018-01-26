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

package io.crate.expression.operator.any;

import io.crate.metadata.FunctionInfo;
import org.apache.lucene.util.BytesRef;

public abstract class AbstractAnyLikeOperator extends AnyOperator {

    @Override
    protected boolean compare(int comparisonResult) {
        return false;
    }

    protected AbstractAnyLikeOperator(FunctionInfo info) {
        super(info);
    }

    @Override
    protected Boolean doEvaluate(Object left, Iterable<?> rightIterable) {
        BytesRef rightBytesRef = (BytesRef) left;
        String pattern = rightBytesRef.utf8ToString();

        boolean hasNull = false;
        for (Object elem : rightIterable) {
            if (elem == null) {
                hasNull = true;
                continue;
            }
            assert elem instanceof BytesRef || elem instanceof String : "elem must be BytesRef or String";

            String elemValue;
            if (elem instanceof BytesRef) {
                elemValue = ((BytesRef) elem).utf8ToString();
            } else {
                elemValue = (String) elem;
            }
            if (matches(elemValue, pattern)) {
                return true;
            }
        }
        return hasNull ? null : false;
    }

    protected abstract boolean matches(String expression, String pattern);
}
