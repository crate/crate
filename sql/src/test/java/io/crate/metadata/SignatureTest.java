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

package io.crate.metadata;

import com.google.common.collect.ImmutableList;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class SignatureTest extends CrateUnitTest {

    @Test
    public void testSimpleMatch() {
        Signature signature = new Signature(DataTypes.STRING);
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING)), is(true));
        assertThat(signature.matches(ImmutableList.of(DataTypes.LONG)), is(false));
    }

    @Test
    public void testMatchUndefined() {
        Signature signature = new Signature(DataTypes.STRING);
        assertThat(signature.matches(ImmutableList.of(DataTypes.UNDEFINED)), is(true));
    }

    @Test
    public void testMatchAny() {
        Signature signature = new Signature(DataTypes.ANY);
        for (DataType dataType : DataTypes.ALL_TYPES) {
            assertThat(signature.matches(ImmutableList.of(dataType)), is(true));
        }
    }

    @Test
    public void testMatchAnyArray() {
        Signature signature = new Signature(DataTypes.ANY_ARRAY);
        assertThat(signature.matches(ImmutableList.of(DataTypes.DOUBLE_ARRAY)), is(true));
        assertThat(signature.matches(ImmutableList.of(DataTypes.OBJECT_ARRAY)), is(true));
    }

    @Test
    public void testMatchAnySet() {
        Signature signature = new Signature(DataTypes.ANY_SET);
        assertThat(signature.matches(ImmutableList.of(new SetType(DataTypes.LONG))), is(true));
        assertThat(signature.matches(ImmutableList.of(new SetType(DataTypes.INTEGER))), is(true));
    }

    @Test
    public void testVarArgsSingleMatching() {
        Signature signature = new Signature(0, DataTypes.STRING);
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.STRING)), is(true));
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.STRING)), is(true));
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.LONG)), is(false));
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.STRING, DataTypes.LONG)), is(false));
    }

    @Test
    public void testVarArgsSequenceMatching() {
        Signature signature = new Signature(0, DataTypes.STRING, DataTypes.LONG);
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.LONG)), is(true));
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.LONG, DataTypes.STRING, DataTypes.LONG)), is(true));
        assertThat(signature.matches(ImmutableList.of(DataTypes.STRING, DataTypes.LONG, DataTypes.STRING)), is(false));
    }
}
