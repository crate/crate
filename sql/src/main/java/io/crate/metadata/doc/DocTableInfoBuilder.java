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
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

class DocTableInfoBuilder {

    private final TableIdent ident;
    private final ClusterState state;
    private ExecutorService executorService;
    private final boolean checkAliasSchema;
    private final Functions functions;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;
    private final MetaData metaData;
    private String[] concreteIndices;
    private static final ESLogger logger = Loggers.getLogger(DocTableInfoBuilder.class);

    DocTableInfoBuilder(Functions functions,
                        TableIdent ident,
                        ClusterService clusterService,
                        IndexNameExpressionResolver indexNameExpressionResolver,
                        TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                        ExecutorService executorService,
                        boolean checkAliasSchema) {
        this.functions = functions;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.ident = ident;
        this.executorService = executorService;
        this.state = clusterService.state();
        this.metaData = state.metaData();
        this.checkAliasSchema = checkAliasSchema;
    }

    private DocIndexMetaData docIndexMetaData() {
        DocIndexMetaData docIndexMetaData;
        String templateName = PartitionName.templateName(ident.schema(), ident.name());
        boolean createdFromTemplate = false;
        if (metaData.getTemplates().containsKey(templateName)) {
            docIndexMetaData = buildDocIndexMetaDataFromTemplate(ident.indexName(), templateName);
            createdFromTemplate = true;
            concreteIndices = indexNameExpressionResolver.concreteIndices(
                state, IndicesOptions.lenientExpandOpen(), ident.indexName());
        } else {
            try {
                concreteIndices = indexNameExpressionResolver.concreteIndices(
                    state, IndicesOptions.strictExpandOpen(), ident.indexName());
                if (concreteIndices.length == 0) {
                    // no matching index found
                    throw new TableUnknownException(ident);
                }
                docIndexMetaData = buildDocIndexMetaData(concreteIndices[0]);
            } catch (IndexNotFoundException ex) {
                throw new TableUnknownException(ident.fqn(), ex);
            }
        }

        if ((!createdFromTemplate && concreteIndices.length == 1) || !checkAliasSchema) {
            return docIndexMetaData;
        }
        for (String concreteIndice : concreteIndices) {
            if (IndexMetaData.State.CLOSE.equals(metaData.indices().get(concreteIndice).getState())) {
                throw new UnhandledServerException(
                    String.format(Locale.ENGLISH, "Unable to access the partition %s, it is closed", concreteIndice));
            }
            try {
                docIndexMetaData = docIndexMetaData.merge(
                    buildDocIndexMetaData(concreteIndice),
                    transportPutIndexTemplateAction,
                    createdFromTemplate);
            } catch (IOException e) {
                throw new UnhandledServerException("Unable to merge/build new DocIndexMetaData", e);
            }
        }
        return docIndexMetaData;
    }

    private DocIndexMetaData buildDocIndexMetaData(String index) {
        DocIndexMetaData docIndexMetaData;
        try {
            docIndexMetaData = new DocIndexMetaData(functions, metaData.index(index), ident);
        } catch (IOException e) {
            throw new UnhandledServerException("Unable to build DocIndexMetaData", e);
        }
        return docIndexMetaData.build();
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
            for (String index : concreteIndices) {
                if (PartitionName.isPartition(index)) {
                    try {
                        PartitionName partitionName = PartitionName.fromIndexOrTemplate(index);
                        assert partitionName.tableIdent().equals(ident) : "ident must equal partitionName";
                        partitions.add(partitionName);
                    } catch (IllegalArgumentException e) {
                        // ignore
                        logger.warn(String.format(Locale.ENGLISH, "Cannot build partition %s of index %s", index, ident.indexName()));
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
            md.indices(),
            md.references(),
            md.analyzers(),
            md.primaryKey(),
            md.routingCol(),
            md.isAlias(),
            md.hasAutoGeneratedPrimaryKey(),
            concreteIndices,
            clusterService,
            indexNameExpressionResolver,
            md.numberOfShards(), md.numberOfReplicas(),
            md.tableParameters(),
            md.partitionedBy(),
            partitions,
            md.columnPolicy(),
            md.getRoutingHashFunction(),
            md.versionCreated(),
            md.versionUpgraded(),
            md.supportedOperations(),
            executorService);
    }
}
