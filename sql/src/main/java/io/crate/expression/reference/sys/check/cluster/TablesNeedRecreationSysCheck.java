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

package io.crate.expression.reference.sys.check.cluster;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import io.crate.blob.v2.BlobIndex;
import io.crate.metadata.TableIdent;
import io.crate.expression.reference.sys.check.AbstractSysCheck;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;
import java.util.TreeSet;

@Singleton
public class TablesNeedRecreationSysCheck extends AbstractSysCheck {

    public static final int ID = 5;
    public static final String DESCRIPTION =
        "The following tables need to be recreated for compatibility with future versions of CrateDB >= 3.0.0: ";

    private final ClusterService clusterService;
    private volatile Collection<String> tablesNeedRecreation;

    @Inject
    public TablesNeedRecreationSysCheck(ClusterService clusterService) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
    }

    @Override
    public BytesRef description() {
        String linkedDescriptionBuilder = DESCRIPTION + tablesNeedRecreation + ' ' + LINK_PATTERN + ID;
        return new BytesRef(linkedDescriptionBuilder);
    }

    @Override
    public boolean validate() {
        tablesNeedRecreation = tablesNeedRecreation(clusterService.state().metaData());
        return tablesNeedRecreation.isEmpty();
    }

    // We need to check for the version that the index was created since the lucene segments might
    // be automatically upgraded to the latest version (can happen when data was in the translog) so
    // minimumCompatVersion cannot be used to indicate that a table recreation is needed.
    @VisibleForTesting
    static boolean isRecreationRequired(String indexName, IndexMetaData indexMetaData) {
        return indexMetaData != null && !BlobIndex.isBlobIndex(indexName) && indexMetaData.getCreationVersion().before(org.elasticsearch.Version.V_5_0_0);
    }

    /**
     * Retrieves an ordered collection of table FQNs that need
     * to be recreated to be compatible with future CrateDB versions.
     * @return the ordered collection of table FQNs that need to be recreated
     */
    static Collection<String> tablesNeedRecreation(MetaData clusterIndexMetaData) {
        Collection<String> tablesNeedRecreation = new TreeSet<>();
        for (ObjectObjectCursor<String, IndexMetaData> entry : clusterIndexMetaData.indices()) {
            String indexName = entry.key;
            IndexMetaData indexMetaData = entry.value;
            if (isRecreationRequired(indexName, indexMetaData)) {
                tablesNeedRecreation.add(TableIdent.fromIndexName(indexName).fqn());
            }
        }
        return tablesNeedRecreation;
    }
}
