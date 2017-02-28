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

package io.crate.operation.reference.sys.check.cluster;

import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.internal.Nullable;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class LuceneVersionChecks {

    static boolean isUpgradeRequired(@Nullable String versionStr) {
        if (versionStr == null || versionStr.isEmpty()) {
            return false;
        }
        try {
            return !Version.parse(versionStr).onOrAfter(Version.LATEST);
        } catch (ParseException e) {
            throw new IllegalArgumentException("'" + versionStr + "' is not a valid Lucene version");
        }
    }

    // We need to check for the version that the index was created since the lucene segments might
    // be automatically upgraded to the latest version (happened when data was in the translog) so
    // minimumCompatVersion cannot be used to indicate that a re-index is needed.
    static boolean isReindexRequired(IndexMetaData indexMetaData) {
        return indexMetaData != null &&
               !indexMetaData.getCreationVersion().luceneVersion.onOrAfter(Version.LUCENE_4_10_0);
    }

    static List<String> tablesNeedReindexing(Schemas schemas, MetaData clusterIndexMetaData) {
        List<String> tablesNeedReindexing = new ArrayList<>();
        for (SchemaInfo schemaInfo : schemas) {
            if (schemaInfo instanceof DocSchemaInfo) {
                for (TableInfo tableInfo : schemaInfo) {
                    if (((DocTableInfo) tableInfo).isPartitioned()) {
                        // check each partition, if one needs reindexing, complete table will be marked
                        for (PartitionName partitionName : ((DocTableInfo) tableInfo).partitions()) {
                            IndexMetaData indexMetaData = clusterIndexMetaData.index(partitionName.asIndexName());
                            assert indexMetaData != null :
                                "could not get metadata for partition " + partitionName.asIndexName();
                            if (checkIndexMetaData(tableInfo, indexMetaData, tablesNeedReindexing)) {
                                break;
                            }
                        }
                    } else {
                        IndexMetaData metaData = ((DocTableInfo) tableInfo).metaData();
                        checkIndexMetaData(tableInfo, metaData, tablesNeedReindexing);
                    }
                }
            }
        }
        Collections.sort(tablesNeedReindexing);
        return tablesNeedReindexing;
    }

    private static boolean checkIndexMetaData(TableInfo tableInfo, IndexMetaData metaData,
                                              List<String> tablesNeedReindexing) {
        if (LuceneVersionChecks.isReindexRequired(metaData)) {
            tablesNeedReindexing.add(tableInfo.ident().fqn());
            return true;
        }
        return false;
    }
}
