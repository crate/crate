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

package io.crate.expression.reference;

import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowCollectExpression;
import org.apache.lucene.util.BytesRef;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class RowCollectNestedObjectExpression<R> extends NestedObjectExpression implements RowCollectExpression<R, Map<String, Object>> {
    protected R row;

    public void setNextRow(R row) {
        this.row = row;
    }

    @Override
    public Map<String, Object> value() {
        Map<String, Object> map = new HashMap<>(childImplementations.size());
        for (Map.Entry<String, ReferenceImplementation> e : childImplementations.entrySet()) {
            ReferenceImplementation referenceImplementation = e.getValue();
            if (referenceImplementation instanceof RowCollectExpression) {
                //noinspection unchecked
                ((RowCollectExpression) referenceImplementation).setNextRow(this.row);
            }
            Object value = referenceImplementation.value();

            // convert nested columns of type e.getValue().value() to String here
            // as we do not want to convert them when building the response
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            map.put(e.getKey(), value);
        }
        return Collections.unmodifiableMap(map);
    }
}
