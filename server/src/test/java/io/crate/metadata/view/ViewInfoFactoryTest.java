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

package io.crate.metadata.view;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.exceptions.RelationsUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.types.ArrayType;
import io.crate.types.TimestampType;

public class ViewInfoFactoryTest {

    @Test
    public void create_view_throws_exception_view_is_created_with_hint_in_statement() {
        ViewInfoFactory factory = new ViewInfoFactory(() -> {
            throw new ConversionException(new ArrayType<>(TimestampType.INSTANCE_WITHOUT_TZ), TimestampType.INSTANCE_WITHOUT_TZ);
        });

        String statement = "SELECT * FROM users";
        RelationName ident = new RelationName(null, "test");
        ViewMetadata viewMetadata = new ViewMetadata(statement, null, SearchPath.pathWithPGCatalogAndDoc());
        ViewsMetadata views = new ViewsMetadata(Map.of(ident.fqn(), viewMetadata));

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().putCustom(ViewsMetadata.TYPE, views)).build();

        // `definition` col includes a hint about an error in the view's query
        ViewInfo viewInfo = factory.create(ident, state);
        assertThat(viewInfo.definition()).isEqualTo("/* Corrupted view, needs fix */\n" + statement);
    }

    @Test
    public void test_view_definition_contains_corrupted_hint_if_analyzer_throws_ResourceUnknownException() {
        ViewInfoFactory factory = new ViewInfoFactory(() -> {
            throw new RelationsUnknown(List.of());
        });

        String statement = "SELECT * FROM users";
        RelationName ident = new RelationName(null, "test");
        ViewMetadata viewMetadata = new ViewMetadata(statement, null, SearchPath.pathWithPGCatalogAndDoc());
        ViewsMetadata views = new ViewsMetadata(Map.of(ident.fqn(), viewMetadata));

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().putCustom(ViewsMetadata.TYPE, views)).build();

        // `definition` col includes a hint about an error in the view's query
        ViewInfo viewInfo = factory.create(ident, state);
        assertThat(viewInfo.definition()).isEqualTo("/* Corrupted view, needs fix */\n" + statement);
    }
}
