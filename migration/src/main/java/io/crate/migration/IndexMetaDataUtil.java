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

package io.crate.migration;

import com.google.common.collect.Maps;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Utility class to load indices metadata and validate versions
 */
final class IndexMetaDataUtil {

    private static final ESLogger LOGGER = Loggers.getLogger(MigrationTool.class);
    private static final String INDEX_STATE_FILE_PREFIX = "state-";

    private IndexMetaDataUtil() {
    }

    static IndexMetaData loadIndexESMetadata(Path path) throws IOException {
        Map<String, String> params = Maps.newHashMap();
        params.put("binary", "true");
        ToXContent.Params formatParams = new ToXContent.MapParams(params);

        return indexStateFormat(XContentType.SMILE, formatParams).loadLatestState(LOGGER, path);
    }

    static boolean checkReindexIsRequired(IndexMetaData indexMetaData) {
        return indexMetaData.getCreationVersion().before(org.elasticsearch.Version.V_1_4_0);
    }

    static boolean checkIndexIsUpgraded(IndexMetaData indexMetaData) {
        return indexMetaData.getUpgradeVersion().onOrAfter(Version.V_2_4_2);
    }

    static boolean checkValidShard(Directory shardDir) {
        try {
            return DirectoryReader.indexExists(shardDir);
        } catch (IOException e) {
            return false;
        }
    }

    static boolean checkAlreadyMigrated(Directory shardDir) throws IOException {
        org.apache.lucene.util.Version luceneVersion = SegmentInfos.readLatestCommit(shardDir).getCommitLuceneVersion();
        return (luceneVersion != null && luceneVersion.onOrAfter(org.apache.lucene.util.Version.LUCENE_5_0_0));
    }

    private static MetaDataStateFormat<IndexMetaData> indexStateFormat(XContentType format, final ToXContent.Params formatParams) {
        return new MetaDataStateFormat<IndexMetaData>(format, INDEX_STATE_FILE_PREFIX) {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                IndexMetaData.Builder.toXContent(state, builder, formatParams);            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
    }
}
