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

package io.crate.metadata.doc;

import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class InternalDocTableInfoFactory implements DocTableInfoFactory {

    private final Functions functions;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Provider<TransportPutIndexTemplateAction> putIndexTemplateActionProvider;

    @Inject
    public InternalDocTableInfoFactory(Functions functions,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       Provider<TransportPutIndexTemplateAction> putIndexTemplateActionProvider) {
        this.functions = functions;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.putIndexTemplateActionProvider = putIndexTemplateActionProvider;
    }

    @Override
    public DocTableInfo create(TableIdent ident, ClusterService clusterService) {
        boolean checkAliasSchema = clusterService.state().metaData().settings().getAsBoolean("crate.table_alias.schema_check", true);
        DocTableInfoBuilder builder = new DocTableInfoBuilder(
            functions,
            ident,
            clusterService,
            indexNameExpressionResolver,
            putIndexTemplateActionProvider.get(),
            checkAliasSchema
        );
        return builder.build();
    }
}
