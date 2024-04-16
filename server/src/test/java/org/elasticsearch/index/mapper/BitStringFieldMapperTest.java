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

package org.elasticsearch.index.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class BitStringFieldMapperTest {

    @Test
    public void test_name_returns_full_path() throws Exception {
        BitStringFieldMapper.Builder builder = new BitStringFieldMapper.Builder("x");
        ContentPath contentPath = new ContentPath();
        contentPath.add("o");
        var context = new Mapper.BuilderContext(contentPath);
        BitStringFieldMapper mapper = builder.build(context);
        assertThat(mapper.name()).isEqualTo("o.x");
    }

    @Test
    public void test_indexed_value_is_applied() {
        BitStringFieldMapper.Builder builder = new BitStringFieldMapper.Builder("x");
        builder.index(false);
        ContentPath contentPath = new ContentPath();
        contentPath.add("o");
        var context = new Mapper.BuilderContext(contentPath);
        BitStringFieldMapper mapper = builder.build(context);
        assertThat(mapper.mappedFieldType.isSearchable()).isFalse();
    }

    @Test
    public void test_hasDocValues_value_is_applied() {
        BitStringFieldMapper.Builder builder = new BitStringFieldMapper.Builder("x");
        builder.docValues(false);
        ContentPath contentPath = new ContentPath();
        contentPath.add("o");
        var context = new Mapper.BuilderContext(contentPath);
        BitStringFieldMapper mapper = builder.build(context);
        assertThat(mapper.mappedFieldType.hasDocValues()).isFalse();
    }

    @Test
    public void test_field_mapper_cannot_have_empty_name() {
        ContentPath contentPath = new ContentPath();
        var context = new Mapper.BuilderContext(contentPath);
        assertThatThrownBy(() -> new BitStringFieldMapper.Builder("").build(context))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("name cannot be empty string");
    }
}
