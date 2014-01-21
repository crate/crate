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

package org.cratedb.export;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.search.Query;
import org.cratedb.action.export.ExportContext;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class to export data of given context
 */
public class Exporter {

    private static final ESLogger logger = Loggers.getLogger(Exporter.class);

    public static class Result {
        public Output.Result outputResult;
        public long numExported;
    }

    private final FetchSubPhase[] fetchSubPhases;
    private final Injector injector;
    private final SettingsFilter settingsFilter;

    private ClusterAdminClient client;

    @Inject
    public Exporter(VersionFetchSubPhase versionPhase, Injector injector,
            SettingsFilter settingsFilter) {
        this.fetchSubPhases = new FetchSubPhase[]{versionPhase};
        this.injector = injector;
        this.settingsFilter = settingsFilter;
    }

    /**
     * Check for permission problems
     *
     * @param context
     * @throws ExportException
     */
    public void check(ExportContext context) throws ExportException {
        if (context.outputFile() != null) {
            File outputFile = new File(context.outputFile());
            File targetFolder = new File(outputFile.getParent());
            if (!targetFolder.exists()) {
                throw new ExportException(context, "Target folder " + outputFile.getParent() + " does not exist");
            }
            if (!targetFolder.canWrite()) {
                throw new ExportException(context, "Insufficient permissions to write into " + outputFile.getParent());
            }
        }
    }

    public Result execute(ExportContext context) {
        if (context.settings() || context.mappings()) {
            writeSettingsOrMappings(context);
        }

        logger.info("exporting {}/{}", context.shardTarget().index(),
                context.shardTarget().getShardId());
        Query query = context.query();

        Output output = context.createOutput();
        context.version(true);
        try {
            output.open();
        } catch (IOException e) {
            throw new ExportException(context, "Failed to open output: ", e);
        }
        ExportCollector collector = new ExportCollector(context, output.getOutputStream(), fetchSubPhases);
        try {
            context.searcher().search(query, collector);
        } catch (IOException e) {
            throw new ExportException(context, "Failed to fetch docs", e);
        }
        try {
            output.close();
        } catch (IOException e) {
            throw new ExportException(context, "Failed to close output: ", e);
        }
        Result res = new Result();
        res.outputResult = output.result();
        res.numExported = collector.numExported();
        logger.info("exported {} docs from {}/{}",
                collector.numExported(),
                context.shardTarget().index(),
                context.shardTarget().getShardId());

        return res;
    }

    private void writeSettingsOrMappings(ExportContext context) {
        if (client == null) {
            client = injector.getInstance(ClusterAdminClient.class);
        }
        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .filterRoutingTable(true)
                .filterNodes(true)
                .filteredIndices(context.shardTarget().index());

        clusterStateRequest.listenerThreaded(false);

        ClusterStateResponse response = client.state(clusterStateRequest).actionGet();
        MetaData metaData = response.getState().metaData();
        IndexMetaData indexMetaData = metaData.iterator().next();
        if (context.settings()) {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.startObject();
                builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);
                builder.startObject("settings");
                Settings settings = settingsFilter.filterSettings(indexMetaData.settings());
                for (Map.Entry<String, String> entry: settings.getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
                builder.endObject();
                builder.endObject();
                File settingsFile = new File(context.outputFile() + ".settings");
                if (!context.forceOverride() && settingsFile.exists()) {
                    throw new IOException("File exists: " + settingsFile.getAbsolutePath());
                }
                OutputStream os = new FileOutputStream(settingsFile);
                os.write(builder.bytes().toBytes());
                os.flush();
                os.close();
            } catch (IOException e) {
                throw new ExportException(context, "Failed to write settings for index " + indexMetaData.index(), e);
            }
        }
        if (context.mappings()) {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.startObject();
                builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);
                Set<String> types = new HashSet<String>(Arrays.asList(context.types()));
                boolean noTypes = types.isEmpty();
                for (ObjectCursor<MappingMetaData> cursor: indexMetaData.mappings().values()) {
                    if (noTypes || types.contains(cursor.value.type())) {
                        builder.field(cursor.value.type());
                        builder.map(cursor.value.sourceAsMap());
                    }
                }
                builder.endObject();
                builder.endObject();
                File mappingsFile = new File(context.outputFile() + ".mapping");
                if (!context.forceOverride() && mappingsFile.exists()) {
                    throw new IOException("File exists: " + mappingsFile.getAbsolutePath());
                }
                OutputStream os = new FileOutputStream(mappingsFile);
                os.write(builder.bytes().toBytes());
                os.flush();
                os.close();
            } catch (IOException e) {
                throw new ExportException(context, "Failed to write mappings for index " + indexMetaData.index(), e);
            }
        }
    }
}
