/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.unit.TimeValue;
import io.crate.exceptions.SQLExceptions;
import io.crate.server.xcontent.XContentHelper;

/**
 * Upgrades Templates on behalf of installed {@link Plugin}s when a node joins the cluster
 */
public class TemplateUpgradeService implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(TemplateUpgradeService.class);

    private final UnaryOperator<Map<String, IndexTemplateMetadata>> indexTemplateMetadataUpgraders;

    public final ClusterService clusterService;

    public final ThreadPool threadPool;

    public final Client client;

    final AtomicInteger upgradesInProgress = new AtomicInteger();

    private ImmutableOpenMap<String, IndexTemplateMetadata> lastTemplateMetadata;

    public TemplateUpgradeService(Client client, ClusterService clusterService, ThreadPool threadPool,
                                  Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexTemplateMetadataUpgraders = templates -> {
            Map<String, IndexTemplateMetadata> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetadata>> upgrader : indexTemplateMetadataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
        if (DiscoveryNode.isMasterEligibleNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        if (upgradesInProgress.get() > 0) {
            // we are already running some upgrades - skip this cluster state update
            return;
        }

        ImmutableOpenMap<String, IndexTemplateMetadata> templates = state.metadata().templates();

        if (templates == lastTemplateMetadata) {
            // we already checked these sets of templates - no reason to check it again
            // we can do identity check here because due to cluster state diffs the actual map will not change
            // if there were no changes
            return;
        }

        lastTemplateMetadata = templates;
        Optional<TemplateDelta> optDelta = calculateTemplateChanges(templates);
        if (optDelta.isPresent()) {
            TemplateDelta templateDelta = optDelta.get();
            Map<String, BytesReference> changes = templateDelta.changes();
            Set<String> deletes = templateDelta.deletes();
            if (upgradesInProgress.compareAndSet(0, changes.size() + changes.size() + 1)) {
                LOGGER.info("Starting template upgrade to version {}, {} templates will be updated and {} will be removed",
                    Version.CURRENT,
                    changes.size(),
                    deletes.size());

                threadPool.generic().execute(() -> upgradeTemplates(changes, deletes));
            }
        }
    }

    void upgradeTemplates(Map<String, BytesReference> changes, Set<String> deletions) {
        final AtomicBoolean anyUpgradeFailed = new AtomicBoolean(false);
        for (Map.Entry<String, BytesReference> change : changes.entrySet()) {
            PutIndexTemplateRequest request =
                new PutIndexTemplateRequest(change.getKey()).source(change.getValue(), XContentType.JSON);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().putTemplate(request).whenComplete((response, err) -> {
                if (err == null) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        LOGGER.warn("Error updating template [{}], request was not acknowledged", change.getKey());
                    }
                } else {
                    anyUpgradeFailed.set(true);
                    LOGGER.warn(new ParameterizedMessage("Error updating template [{}]", change.getKey()), err);
                }
                tryFinishUpgrade(anyUpgradeFailed);
            });
        }

        for (String template : deletions) {
            DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest(template);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().deleteTemplate(request).whenComplete((response, err) -> {
                if (err == null) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        LOGGER.warn("Error deleting template [{}], request was not acknowledged", template);
                    }
                } else {
                    err = SQLExceptions.unwrap(err);
                    anyUpgradeFailed.set(true);
                    if (err instanceof IndexTemplateMissingException == false) {
                        // we might attempt to delete the same template from different nodes - so that's ok if template doesn't exist
                        // otherwise we need to warn
                        LOGGER.warn(new ParameterizedMessage("Error deleting template [{}]", template), err);
                    }
                }
                tryFinishUpgrade(anyUpgradeFailed);
            });
        }
    }

    void tryFinishUpgrade(AtomicBoolean anyUpgradeFailed) {
        assert upgradesInProgress.get() > 0;
        if (upgradesInProgress.decrementAndGet() == 1) {
            try {
                // this is the last upgrade, the templates should now be in the desired state
                if (anyUpgradeFailed.get()) {
                    LOGGER.info("Templates were partially upgraded to version {}", Version.CURRENT);
                } else {
                    LOGGER.info("Templates were upgraded successfully to version {}", Version.CURRENT);
                }
                // Check upgraders are satisfied after the update completed. If they still
                // report that changes are required, this might indicate a bug or that something
                // else tinkering with the templates during the upgrade.
                final ImmutableOpenMap<String, IndexTemplateMetadata> upgradedTemplates =
                        clusterService.state().metadata().templates();
                final boolean changesRequired = calculateTemplateChanges(upgradedTemplates).isPresent();
                if (changesRequired) {
                    LOGGER.warn("Templates are still reported as out of date after the upgrade. The template upgrade will be retried.");
                }
            } finally {
                final int noMoreUpgrades = upgradesInProgress.decrementAndGet();
                assert noMoreUpgrades == 0;
            }
        }
    }

    record TemplateDelta(Map<String, BytesReference> changes, Set<String> deletes) {}

    Optional<TemplateDelta> calculateTemplateChanges(
        ImmutableOpenMap<String, IndexTemplateMetadata> templates) {
        // collect current templates
        Map<String, IndexTemplateMetadata> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, IndexTemplateMetadata> customCursor : templates) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, IndexTemplateMetadata> upgradedMap = indexTemplateMetadataUpgraders.apply(existingMap);
        if (upgradedMap.equals(existingMap) == false) {
            Set<String> deletes = new HashSet<>();
            Map<String, BytesReference> changes = new HashMap<>();
            // remove templates if needed
            existingMap.keySet().forEach(s -> {
                if (upgradedMap.containsKey(s) == false) {
                    deletes.add(s);
                }
            });
            upgradedMap.forEach((key, value) -> {
                if (value.equals(existingMap.get(key)) == false) {
                    changes.put(key, toBytesReference(value));
                }
            });
            return Optional.of(new TemplateDelta(changes, deletes));
        }
        return Optional.empty();
    }

    private static final ToXContent.Params PARAMS = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

    private BytesReference toBytesReference(IndexTemplateMetadata templateMetadata) {
        try {
            return XContentHelper.toXContent((builder, params) -> {
                IndexTemplateMetadata.Builder.toInnerXContent(templateMetadata, builder, params);
                return builder;
            }, XContentType.JSON, PARAMS, false);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot serialize template [" + templateMetadata.getName() + "]", ex);
        }
    }
}
