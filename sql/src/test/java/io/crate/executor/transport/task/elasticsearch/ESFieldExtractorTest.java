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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ESFieldExtractorTest {

    @Test
    public void testPath2() throws Exception {

        ESFieldExtractor.Source ex = new ESFieldExtractor.Source(
                new ColumnIdent("top", "child1"), DataTypes.INTEGER);
        Map<String, Object> source;

        source = ImmutableMap.of();
        assertNull(ex.toValue(source));

        source = ImmutableMap.<String, Object>of(
                "top", ImmutableMap.of("child1", 1, "child2", 2)
        );
        assertEquals(1, ex.toValue(source));


        ex = new ESFieldExtractor.Source(
                new ColumnIdent("top", "child1"), new ArrayType(DataTypes.INTEGER));
        source = ImmutableMap.<String, Object>of(
                "top", ImmutableList.of(
                ImmutableMap.of("child1", 1),
                ImmutableMap.of("child1", 2),
                ImmutableMap.of("child2", 22))
        );
        assertThat((Integer) ((Object[]) ex.toValue(source))[0], is(1));
        assertThat((Integer) ((Object[]) ex.toValue(source))[1], is(2));

        // if the container is present we get an empty list instead of null, to reflect the container exitence
        source = ImmutableMap.<String, Object>of(
                "top", ImmutableList.of(
                ImmutableMap.of("child2", 22),
                ImmutableMap.of("child3", 33))
        );
        assertThat(((Object[]) ex.toValue(source)).length, is(0));

        // if the container does not match -> null
        source = ImmutableMap.<String, Object>of(
                "nomatch", ImmutableList.of(
                ImmutableMap.of("child2", 22),
                ImmutableMap.of("child3", 33))
        );
        assertNull(ex.toValue(source));

    }

    @Test
    public void testPath3() throws Exception {
        ColumnIdent ci = new ColumnIdent("a", ImmutableList.of("b", "c"));
        ESFieldExtractor.Source ex = new ESFieldExtractor.Source(ci, DataTypes.INTEGER);
        Map<String, Object> source;

        source = ImmutableMap.<String, Object>of(
                "a", ImmutableMap.of("b", ImmutableMap.of("c", 1)
        ));
        assertEquals(1, ex.toValue(source));

        source = ImmutableMap.<String, Object>of(
                "a", ImmutableMap.of("b", ImmutableMap.of("d", 1)
        ));
        assertEquals(null, ex.toValue(source));

        source = ImmutableMap.<String, Object>of(
                "a", ImmutableMap.of("b", ImmutableMap.of("c", 1, "d", 2)
        ));
        assertEquals(1, ex.toValue(source));
    }

    @Test
    public void testNullInList() throws Exception {
        ESFieldExtractor.Source ex = new ESFieldExtractor.Source(new ColumnIdent("top", "child1"), new ArrayType(DataTypes.INTEGER));
        // test null value in list
        HashMap<String, Object> nullMap = new HashMap<String, Object>(1);
        nullMap.put("child1", null);
        ImmutableMap<String, Object> source = ImmutableMap.<String, Object>of(
                "top", ImmutableList.of(
                        nullMap,
                        ImmutableMap.of("child1", 33))
        );
        Object[] objects = (Object[]) ex.toValue(source);
        assertThat(objects[0], nullValue());
        assertThat(((Integer) objects[1]), is(33));
    }

}
