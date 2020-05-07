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

package io.crate.metadata.doc;

import io.crate.Constants;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

class DocTableInfoBuilder {

    private final RelationName ident;
    private final ClusterState state;
    private final Functions functions;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final MetaData metaData;
    private String[] concreteIndices;
    private String[] concreteOpenIndices;
    private static final Logger LOGGER = LogManager.getLogger(DocTableInfoBuilder.class);

    DocTableInfoBuilder(Functions functions,
                        RelationName ident,
                        ClusterState state,
                        IndexNameExpressionResolver indexNameExpressionResolver) {
        this.functions = functions;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.ident = ident;
        this.state = state;
        this.metaData = state.metaData();
    }

    private DocIndexMetaData docIndexMetaData() {
        DocIndexMetaData docIndexMetaData;
        String templateName = PartitionName.templateName(ident.schema(), ident.name());
        if (metaData.getTemplates().containsKey(templateName)) {
            docIndexMetaData = buildDocIndexMetaDataFromTemplate(ident.indexNameOrAlias(), templateName);
            // We need all concrete indices, regardless of their state, for operations such as reopening.
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                state, IndicesOptions.lenientExpandOpen(), ident.indexNameOrAlias());
            // We need all concrete open indices, as closed indices must not appear in the routing.
            concreteOpenIndices = indexNameExpressionResolver.concreteIndexNames(
                state, IndicesOptions.fromOptions(true, true, true,
                    false, IndicesOptions.strictExpandOpenAndForbidClosed()), ident.indexNameOrAlias());
        } else {
            try {
                concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                    state, IndicesOptions.strictExpandOpen(), ident.indexNameOrAlias());
                concreteOpenIndices = concreteIndices;
                if (concreteIndices.length == 0) {
                    // no matching index found
                    throw new RelationUnknown(ident);
                }
                docIndexMetaData = buildDocIndexMetaData(concreteIndices[0]);
            } catch (IndexNotFoundException ex) {
                throw new RelationUnknown(ident.fqn(), ex);
            }
        }
        return docIndexMetaData;
    }

    private DocIndexMetaData buildDocIndexMetaData(String indexName) {
        DocIndexMetaData docIndexMetaData;
        IndexMetaData indexMetaData = metaData.index(indexName);
        try {
            docIndexMetaData = new DocIndexMetaData(functions, indexMetaData, ident);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetaData", e);
        }
        try {
            return docIndexMetaData.build();
        } catch (Exception e) {
            try {
                LOGGER.error(
                    "Could not build DocIndexMetaData from: {}", indexMetaData.mapping("default").getSourceAsMap());
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    private DocIndexMetaData buildDocIndexMetaDataFromTemplate(String index, String templateName) {
        IndexTemplateMetaData indexTemplateMetaData = metaData.getTemplates().get(templateName);
        DocIndexMetaData docIndexMetaData;
        try {
            IndexMetaData.Builder builder = new IndexMetaData.Builder(index);
            builder.putMapping(Constants.DEFAULT_MAPPING_TYPE,
                indexTemplateMetaData.getMappings().get(Constants.DEFAULT_MAPPING_TYPE).toString());

            Settings.Builder settingsBuilder = Settings.builder()
                .put(indexTemplateMetaData.settings())
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT);

            Settings settings = settingsBuilder.build();
            builder.settings(settings);
            builder.numberOfShards(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
            builder.numberOfReplicas(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
            docIndexMetaData = new DocIndexMetaData(functions, builder.build(), ident);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetaData from template", e);
        }
        return docIndexMetaData.build();
    }

    private List<PartitionName> buildPartitions(DocIndexMetaData md) {
        List<PartitionName> partitions = new ArrayList<>();
        if (md.partitionedBy().size() > 0) {
            for (String indexName : concreteIndices) {
                if (IndexParts.isPartitioned(indexName)) {
                    try {
                        PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexName);
                        assert partitionName.relationName().equals(ident) : "ident must equal partitionName";
                        partitions.add(partitionName);
                    } catch (IllegalArgumentException e) {
                        // ignore
                        LOGGER.warn(String.format(Locale.ENGLISH, "Cannot build partition %s of index %s", indexName, ident.indexNameOrAlias()));
                    }
                }
            }
        }
        return partitions;
    }

    public DocTableInfo build() {
        DocIndexMetaData md = docIndexMetaData();
        List<PartitionName> partitions = buildPartitions(md);

        return new DocTableInfo(
            ident,
            md.columns(),
            md.partitionedByColumns(),
            md.generatedColumnReferences(),
            md.notNullColumns(),
            md.indices(),
            md.references(),
            md.analyzers(),
            md.primaryKey(),
            md.checkConstraints(),
            md.routingCol(),
            md.hasAutoGeneratedPrimaryKey(),
            concreteIndices,
            concreteOpenIndices,
            indexNameExpressionResolver,
            md.numberOfShards(), md.numberOfReplicas(),
            md.tableParameters(),
            md.partitionedBy(),
            partitions,
            md.columnPolicy(),
            md.versionCreated(),
            md.versionUpgraded(),
            md.isClosed(),
            md.supportedOperations());
    }
}
