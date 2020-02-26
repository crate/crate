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

package io.crate.expression.reference.sys;

import io.crate.expression.NestableInput;
import io.crate.expression.reference.NestedObjectExpression;
import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SysExpressionsBytesRefTest extends CrateUnitTest {

    static class BytesRefNullSysExpression implements NestableInput<BytesRef> {

        @Override
        public BytesRef value() {
            return null;
        }

    }

    static class NullScopedSymbolSysObjectReference extends NestedObjectExpression {

        protected NullScopedSymbolSysObjectReference() {
            childImplementations.put("n", new BytesRefNullSysExpression());
        }

    }


    static class NullSysObjectArrayReference extends SysStaticObjectArrayReference {

        protected NullSysObjectArrayReference() {
            childImplementations.add(new NullScopedSymbolSysObjectReference());
        }
    }

    @Test
    public void testSysObjectReferenceNull() throws Exception {
        NullScopedSymbolSysObjectReference nullRef = new NullScopedSymbolSysObjectReference();
        NestableInput n = nullRef.getChild("n");
        assertThat(n, instanceOf(BytesRefNullSysExpression.class));

        Map<String, Object> value = nullRef.value();
        assertThat(value.size(), is(1));
        assertThat(value, hasKey("n"));
        assertThat(value.get("n"), is(nullValue()));
    }

    @Test
    public void testSysObjectArrayReferenceNull() throws Exception {
        NullSysObjectArrayReference nullArrayRef = new NullSysObjectArrayReference();
        List<Object> values = nullArrayRef.value();
        assertThat(values.size(), is(1));
        assertThat(values.get(0), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> mapValue = (Map<String, Object>) values.get(0);

        assertThat(mapValue.size(), is(1));
        assertThat(mapValue, hasKey("n"));
        assertThat(mapValue.get("n"), is(nullValue()));
    }

}
