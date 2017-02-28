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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.metadata.TableIdent;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.internal.Nullable;

import java.text.ParseException;
import java.util.Collection;
import java.util.TreeSet;

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

    /**
     * Retrieves an order collection of table FQNs that need
     * to be re-indexed to be compatible with future CrateDB versions.
     * @param clusterIndexMetaData
     * @return the ordered collection of table FQNs that need re-indexing
     */
    static Collection<String> tablesNeedReindexing(MetaData clusterIndexMetaData) {
        Collection<String> tablesNeedReindexing = new TreeSet<>();
        for (ObjectObjectCursor<String, IndexMetaData> entry : clusterIndexMetaData.indices()) {
            checkIndexMetaData(entry.key, entry.value, tablesNeedReindexing);
        }
        return tablesNeedReindexing;
    }

    private static void checkIndexMetaData(String index, IndexMetaData metaData,
                                           Collection<String> tablesNeedReindexing) {
        if (LuceneVersionChecks.isReindexRequired(metaData)) {
            tablesNeedReindexing.add(TableIdent.fromIndexName(index).fqn());
        }
    }
}
