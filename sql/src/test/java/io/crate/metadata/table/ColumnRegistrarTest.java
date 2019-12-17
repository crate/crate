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

package io.crate.metadata.table;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.hamcrest.Matchers;
import org.junit.Test;

import static io.crate.types.DataTypes.LONG;
import static org.hamcrest.MatcherAssert.assertThat;

public class ColumnRegistrarTest {

    @Test
    public void test_array_within_object_has_array_type() {
        var columns = new ColumnRegistrar<>(new RelationName("doc", "dummy"), RowGranularity.DOC);
        columns.register(
            "fs",
            ObjectType.builder()
                .setInnerType("total", ObjectType.builder().setInnerType("size", LONG).build())
                .setInnerType("data", new ArrayType(ObjectType.builder().setInnerType("path", DataTypes.STRING).build()))
                .build(),
            () -> null
        );
        Reference reference = columns.infos().get(new ColumnIdent("fs", "data"));
        assertThat(reference.valueType(), Matchers.is(new ArrayType(ObjectType.untyped())));
    }
}
