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

package io.crate.executor;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BytesRefUtils;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;

public class BytesRefUtilsTest extends CrateUnitTest {

    final static Joiner commaJoiner = Joiner.on(", ");

    @Test
    public void testEnsureStringTypesAreStringsSetString() throws Exception {
        DataType[] dataTypes = new DataType[]{new SetType(DataTypes.STRING)};
        Object[][] rows = new Object[1][1];
        Set<BytesRef> refs = new HashSet<>(
            Arrays.asList(new BytesRef("foo"), new BytesRef("bar")));

        rows[0][0] = refs;
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);
        assertThat((String[]) rows[0][0], Matchers.arrayContainingInAnyOrder("foo", "bar"));
    }

    @Test
    public void testEnsureStringTypesAreStringsArrayString() throws Exception {
        DataType[] dataTypes = new DataType[]{new ArrayType(DataTypes.STRING)};
        Object[][] rows = new Object[1][1];
        BytesRef[] refs = new BytesRef[]{new BytesRef("foo"), new BytesRef("bar")};

        rows[0][0] = refs;
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);
        assertThat(commaJoiner.join((String[]) rows[0][0]), is("foo, bar"));
    }

    @Test
    public void testConvertSetWithNullValues() throws Exception {
        DataType[] dataTypes = new DataType[]{new SetType(DataTypes.STRING)};
        Object[][] rows = new Object[1][1];
        Set<BytesRef> refs = new HashSet<>(
            Arrays.asList(new BytesRef("foo"), null));
        rows[0][0] = refs;
        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);

        assertThat((String[]) rows[0][0], Matchers.arrayContainingInAnyOrder("foo", null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertObjectValues() throws Exception {
        DataType[] dataTypes = new DataType[]{ DataTypes.OBJECT };
        Object[][] rows = new Object[1][1];
        rows[0][0] = ImmutableMap.<String, Object>builder()
            .put("str", BytesRefs.toBytesRef("string value"))
            .put("str_array", new BytesRef[]{ BytesRefs.toBytesRef("v1"), null })
            .put("nested", ImmutableMap.builder()
                .put("str", BytesRefs.toBytesRef("other value"))
                .put("str_array", new BytesRef[]{ null, BytesRefs.toBytesRef("v2") })
                .put("obj_array", new Map[]{
                    Collections.singletonMap("key", BytesRefs.toBytesRef("value"))
                })
                .build())
            .build();

        BytesRefUtils.ensureStringTypesAreStrings(dataTypes, rows);

        Map<String, Object> result = (Map<String, Object>) rows[0][0];
        assertThat(result.get("str"), is("string value"));
        assertThat((String[]) result.get("str_array"), Matchers.arrayContainingInAnyOrder("v1", null));
        result = (Map<String, Object>) result.get("nested");
        assertThat(result.get("str"), is("other value"));
        assertThat((String[]) result.get("str_array"), Matchers.arrayContainingInAnyOrder("v2", null));
        assertThat(result.get("obj_array"), is(new Map[]{ Collections.singletonMap("key", "value") }));
    }

}
