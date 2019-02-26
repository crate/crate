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

package io.crate.es.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import io.crate.es.Version;
import io.crate.es.action.ActionListener;
import io.crate.es.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import io.crate.es.action.admin.indices.template.put.PutIndexTemplateRequest;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.client.Client;
import io.crate.es.cluster.ClusterChangedEvent;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.ClusterStateListener;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.bytes.BytesReference;
import io.crate.es.common.collect.ImmutableOpenMap;
import io.crate.es.common.collect.Tuple;
import io.crate.es.common.component.AbstractComponent;
import io.crate.es.common.settings.Settings;
import io.crate.es.common.unit.TimeValue;
import io.crate.es.common.util.concurrent.ThreadContext;
import io.crate.es.common.xcontent.ToXContent;
import io.crate.es.common.xcontent.XContentHelper;
import io.crate.es.common.xcontent.XContentType;
import io.crate.es.gateway.GatewayService;
import io.crate.es.indices.IndexTemplateMissingException;
import io.crate.es.plugins.Plugin;
import io.crate.es.threadpool.ThreadPool;

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

import static java.util.Collections.singletonMap;

/**
 * Upgrades Templates on behalf of installed {@link Plugin}s when a node joins the cluster
 */
public class TemplateUpgradeService extends AbstractComponent implements ClusterStateListener {
    private final UnaryOperator<Map<String, IndexTemplateMetaData>> indexTemplateMetaDataUpgraders;

    public final ClusterService clusterService;

    public final ThreadPool threadPool;

    public final Client client;

    final AtomicInteger upgradesInProgress = new AtomicInteger();

    private ImmutableOpenMap<String, IndexTemplateMetaData> lastTemplateMetaData;

    public TemplateUpgradeService(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool,
                                  Collection<UnaryOperator<Map<String, IndexTemplateMetaData>>> indexTemplateMetaDataUpgraders) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexTemplateMetaDataUpgraders = templates -> {
            Map<String, IndexTemplateMetaData> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetaData>> upgrader : indexTemplateMetaDataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        if (upgradesInProgress.get() > 0) {
            // we are already running some upgrades - skip this cluster state update
            return;
        }

        ImmutableOpenMap<String, IndexTemplateMetaData> templates = state.getMetaData().getTemplates();

        if (templates == lastTemplateMetaData) {
            // we already checked these sets of templates - no reason to check it again
            // we can do identity check here because due to cluster state diffs the actual map will not change
            // if there were no changes
            return;
        }

        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        lastTemplateMetaData = templates;
        Optional<Tuple<Map<String, BytesReference>, Set<String>>> changes = calculateTemplateChanges(templates);
        if (changes.isPresent()) {
            if (upgradesInProgress.compareAndSet(0, changes.get().v1().size() + changes.get().v2().size() + 1)) {
                logger.info("Starting template upgrade to version {}, {} templates will be updated and {} will be removed",
                    Version.CURRENT,
                    changes.get().v1().size(),
                    changes.get().v2().size());

                final ThreadContext threadContext = threadPool.getThreadContext();
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    threadContext.markAsSystemContext();
                    threadPool.generic().execute(() -> upgradeTemplates(changes.get().v1(), changes.get().v2()));
                }
            }
        }
    }

    void upgradeTemplates(Map<String, BytesReference> changes, Set<String> deletions) {
        final AtomicBoolean anyUpgradeFailed = new AtomicBoolean(false);
        if (threadPool.getThreadContext().isSystemContext() == false) {
            throw new IllegalStateException("template updates from the template upgrade service should always happen in a system context");
        }

        for (Map.Entry<String, BytesReference> change : changes.entrySet()) {
            PutIndexTemplateRequest request =
                new PutIndexTemplateRequest(change.getKey()).source(change.getValue(), XContentType.JSON);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().putTemplate(request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        logger.warn("Error updating template [{}], request was not acknowledged", change.getKey());
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }

                @Override
                public void onFailure(Exception e) {
                    anyUpgradeFailed.set(true);
                    logger.warn(new ParameterizedMessage("Error updating template [{}]", change.getKey()), e);
                    tryFinishUpgrade(anyUpgradeFailed);
                }
            });
        }

        for (String template : deletions) {
            DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest(template);
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().deleteTemplate(request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        logger.warn("Error deleting template [{}], request was not acknowledged", template);
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }

                @Override
                public void onFailure(Exception e) {
                    anyUpgradeFailed.set(true);
                    if (e instanceof IndexTemplateMissingException == false) {
                        // we might attempt to delete the same template from different nodes - so that's ok if template doesn't exist
                        // otherwise we need to warn
                        logger.warn(new ParameterizedMessage("Error deleting template [{}]", template), e);
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }
            });
        }
    }

    void tryFinishUpgrade(AtomicBoolean anyUpgradeFailed) {
        assert upgradesInProgress.get() > 0;
        if (upgradesInProgress.decrementAndGet() == 1) {
            try {
                // this is the last upgrade, the templates should now be in the desired state
                if (anyUpgradeFailed.get()) {
                    logger.info("Templates were partially upgraded to version {}", Version.CURRENT);
                } else {
                    logger.info("Templates were upgraded successfully to version {}", Version.CURRENT);
                }
                // Check upgraders are satisfied after the update completed. If they still
                // report that changes are required, this might indicate a bug or that something
                // else tinkering with the templates during the upgrade.
                final ImmutableOpenMap<String, IndexTemplateMetaData> upgradedTemplates =
                        clusterService.state().getMetaData().getTemplates();
                final boolean changesRequired = calculateTemplateChanges(upgradedTemplates).isPresent();
                if (changesRequired) {
                    logger.warn("Templates are still reported as out of date after the upgrade. The template upgrade will be retried.");
                }
            } finally {
                final int noMoreUpgrades = upgradesInProgress.decrementAndGet();
                assert noMoreUpgrades == 0;
            }
        }
    }

    Optional<Tuple<Map<String, BytesReference>, Set<String>>> calculateTemplateChanges(
        ImmutableOpenMap<String, IndexTemplateMetaData> templates) {
        // collect current templates
        Map<String, IndexTemplateMetaData> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, IndexTemplateMetaData> customCursor : templates) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, IndexTemplateMetaData> upgradedMap = indexTemplateMetaDataUpgraders.apply(existingMap);
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
            return Optional.of(new Tuple<>(changes, deletes));
        }
        return Optional.empty();
    }

    private static final ToXContent.Params PARAMS = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

    private BytesReference toBytesReference(IndexTemplateMetaData templateMetaData) {
        try {
            return XContentHelper.toXContent((builder, params) -> {
                IndexTemplateMetaData.Builder.toInnerXContent(templateMetaData, builder, params);
                return builder;
            }, XContentType.JSON, PARAMS, false);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot serialize template [" + templateMetaData.getName() + "]", ex);
        }
    }
}
