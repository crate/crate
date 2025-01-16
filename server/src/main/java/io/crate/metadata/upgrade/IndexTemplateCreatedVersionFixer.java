/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.upgrade;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;

import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;

public class IndexTemplateCreatedVersionFixer implements UnaryOperator<Metadata> {

    @Override
    public Metadata apply(Metadata metadata) {
        var templates = metadata.templates();
        var metadataBuilder = new Metadata.Builder(metadata);
        for (Iterator<String> it = templates.keysIt(); it.hasNext(); ) {
            String templateName = it.next();
            List<Version> partitionVersionsCreated = StreamSupport.stream(metadata.indices().spliterator(), false)
                .filter(e -> e.key.startsWith(templateName))
                .map(e -> e.value)
                .map(IndexMetadata::getCreationVersion).sorted().distinct().toList();
            if (partitionVersionsCreated.size() <= 1) {
                continue;
            }
            final Version lowestPartionVersion = partitionVersionsCreated.getFirst();
            IndexTemplateMetadata indexTemplateMetadata = templates.get(templateName);
            if (!lowestPartionVersion.equals(indexTemplateMetadata.settings().getAsVersion(SETTING_VERSION_CREATED, Version.CURRENT))) {
                Settings upgradedSettings = Settings.builder()
                    .put(indexTemplateMetadata.settings())
                    .put(SETTING_VERSION_CREATED, lowestPartionVersion)
                    .build();
                IndexTemplateMetadata upgradedIndexTemplateMetadata = new IndexTemplateMetadata.Builder(indexTemplateMetadata)
                    .settings(upgradedSettings)
                    .build();
                metadataBuilder.put(upgradedIndexTemplateMetadata);
                metadata = metadataBuilder.build();
            }
        }
        return metadata;
    }
}
