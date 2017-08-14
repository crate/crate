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

package io.crate.metadata.rule.ingest;

import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.cluster.DDLClusterStateService;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class IngestionService extends AbstractLifecycleComponent implements ClusterStateListener {

    private final ClusterService clusterService;
    private final Map<String, List<IngestionImplementation>> implementations = new HashMap<>();
    private final Schemas schemas;

    @Inject
    public IngestionService(Settings settings,
                            Schemas schemas,
                            ClusterService clusterService,
                            DDLClusterStateService ddlClusterStateService) {
        super(settings);
        this.schemas = schemas;
        this.clusterService = clusterService;
        ddlClusterStateService.addModifier(new IngestionDDLClusterStateModifier());
    }

    /**
     * Register the provided @param implementation for ingest rule changes belonging to the provided @param sourceIdent.
     */
    public void registerImplementation(String sourceIdent, IngestionImplementation implementation) {
        List<IngestionImplementation> implementationsForSource = implementations.computeIfAbsent(sourceIdent, k -> new ArrayList<>());
        implementationsForSource.add(implementation);
        implementation.applyRules(getIngestionRules(clusterService.state().metaData()).get(sourceIdent));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.metaDataChanged() == false) {
            return;
        }

        Map<String, Set<IngestRule>> ingestionRules = getIngestionRules(event.state().metaData());
        filterOutInvalidRules(ingestionRules);
        for (Map.Entry<String, List<IngestionImplementation>> entry : implementations.entrySet()) {
            String sourceIdent = entry.getKey();
            Set<IngestRule> rulesForSource = ingestionRules.get(sourceIdent);
            for (IngestionImplementation ingestionImplementation : entry.getValue()) {
                ingestionImplementation.applyRules(rulesForSource);
            }
        }
    }

    private void filterOutInvalidRules(Map<String, Set<IngestRule>> ingestionRules) {
        for (Set<IngestRule> sourceIngestRules : ingestionRules.values()) {
            Iterator<IngestRule> rulesIterator = sourceIngestRules.iterator();
            //noinspection Java8CollectionRemoveIf - would result in a lambda instance for every iteration
            while (rulesIterator.hasNext()) {
                IngestRule ingestRule = rulesIterator.next();
                if (schemas.tableExists(TableIdent.fromIndexName(ingestRule.getTargetTable())) == false) {
                    rulesIterator.remove();
                }
            }
        }
    }

    private Map<String, Set<IngestRule>> getIngestionRules(MetaData metaData) {
        IngestRulesMetaData ingestRulesMetaData =
            (IngestRulesMetaData) metaData.customs().get(IngestRulesMetaData.TYPE);

        if (ingestRulesMetaData != null) {
            return ingestRulesMetaData.getIngestRules();
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() throws IOException {

    }
}
