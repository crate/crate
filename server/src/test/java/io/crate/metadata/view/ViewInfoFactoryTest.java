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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.inject.Provider;
import org.junit.Test;

import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.RelationsUnknown;
import io.crate.metadata.RelationName;
import io.crate.metadata.SearchPath;
import io.crate.types.ArrayType;
import io.crate.types.TimestampType;

public class ViewInfoFactoryTest {

    @Test
    public void create_view_throws_exception_view_is_created_with_hint_in_statement() {
        Provider<RelationAnalyzer> analyzerProvider = mock(Provider.class);
        when(analyzerProvider.get()).thenThrow(
            new ConversionException(new ArrayType<>(TimestampType.INSTANCE_WITHOUT_TZ), TimestampType.INSTANCE_WITHOUT_TZ)
        );
        ViewInfoFactory factory = new ViewInfoFactory(analyzerProvider);

        ViewsMetadata viewsMetadata = mock(ViewsMetadata.class);
        String statement = "SELECT * FROM users";
        RelationName ident = new RelationName(null, "test");
        when(viewsMetadata.getView(ident)).thenReturn(new ViewMetadata(statement, null, SearchPath.pathWithPGCatalogAndDoc()));

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().putCustom(ViewsMetadata.TYPE, viewsMetadata)).build();

        // `definition` col includes a hint about an error in the view's query
        ViewInfo viewInfo = factory.create(ident, state);
        assertThat(viewInfo.definition()).isEqualTo("/* Corrupted view, needs fix */\n" + statement);
    }

    @Test
    public void create_view_throws_ResourceUnknownException_view_is_created_without_hint_in_statement() {
        //TODO: Add hint on ResourceUnknownException as well, once https://github.com/crate/crate/issues/14382 is fixed.
        Provider<RelationAnalyzer> analyzerProvider = mock(Provider.class);
        when(analyzerProvider.get()).thenThrow(new RelationsUnknown(List.of()));
        ViewInfoFactory factory = new ViewInfoFactory(analyzerProvider);

        ViewsMetadata viewsMetadata = mock(ViewsMetadata.class);
        String statement = "SELECT * FROM users";
        RelationName ident = new RelationName(null, "test");
        when(viewsMetadata.getView(ident)).thenReturn(new ViewMetadata(statement, null, SearchPath.pathWithPGCatalogAndDoc()));

        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().putCustom(ViewsMetadata.TYPE, viewsMetadata)).build();

        // `definition` col includes a hint about an error in the view's query
        ViewInfo viewInfo = factory.create(ident, state);
        assertThat(viewInfo.definition()).isEqualTo(statement);
    }

}
