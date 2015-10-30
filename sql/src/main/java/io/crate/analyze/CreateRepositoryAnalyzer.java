/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.crate.sql.tree.CreateRepository;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

@Singleton
public class CreateRepositoryAnalyzer extends AbstractRepositoryDDLAnalyzer<CreateRepositoryAnalyzedStatement, CreateRepository> {

    private final ParamValidator paramValidator;

    @Inject
    CreateRepositoryAnalyzer(ClusterService clusterService) {
        super(clusterService);
        this.paramValidator = new ParamValidator();
    }

    @Override
    public CreateRepositoryAnalyzedStatement visitCreateRepository(CreateRepository node, Analysis context) {
        String repositoryName = node.repository();
        failIfRepositoryExists(repositoryName);

        Settings settings = ImmutableSettings.EMPTY;
        if (node.properties().isPresent()) {
            settings = GenericPropertiesConverter.genericPropertiesToSettings(node.properties().get(), context.parameterContext());
        }
        paramValidator.validate(node.type(), settings);
        return new CreateRepositoryAnalyzedStatement(repositoryName, node.type(), settings);
    }

    static class ParamValidator {
        private final ImmutableMap<String, TypeSettings> typeSettings;

        public ParamValidator() {
            typeSettings = ImmutableMap.<String, TypeSettings>builder()
                    .put("fs", new TypeSettings(ImmutableSet.of("location"),  ImmutableSet.of("compress", "chunk_size")))
                    .put("hdfs", new TypeSettings(ImmutableSet.<String>of(), ImmutableSet.of(
                            "uri", "user", "path", "load_defaults", "conf_location", "concurrent_streams", "compress", "chunk_size")))
                    .put("url", new TypeSettings(ImmutableSet.of("url"), ImmutableSet.<String>of()))
                    .build();
        }

        /**
         * validates the type and settings
         * @throws IllegalArgumentException if the type is invalid, required settings are missing or invalid settings are provided
         */
        public void validate(String type, Settings settings) throws IllegalArgumentException {
            TypeSettings typeSettings = this.typeSettings.get(type);
            if (typeSettings == null) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid repository type \"%s\"", type));
            }
            Set<String> names = settings.getAsMap().keySet();
            Sets.SetView<String> missingRequiredSettings = Sets.difference(typeSettings.required, names);
            if (!missingRequiredSettings.isEmpty()) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "The following required parameters are missing to create a repository of type \"%s\": [%s]",
                        type, Joiner.on(", ").join(missingRequiredSettings)));
            }

            Set<String> invalidSettings = removeDynamicHdfsConfSettings(type, Sets.difference(names, typeSettings.all));
            if (!invalidSettings.isEmpty()) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid parameters specified: [%s]",
                        Joiner.on(", ").join(invalidSettings)));
            }
        }

        private Set<String> removeDynamicHdfsConfSettings(String type, Sets.SetView<String> invalidSettings) {
            // hdfs supports dynamic params conf.<key>
            // don't want to fail just because of those
            if (!invalidSettings.isEmpty() && type.equalsIgnoreCase("hdfs")) {
                Set<String> newSet = new HashSet<>(invalidSettings.size());
                for (String s : invalidSettings.immutableCopy()) {
                    if (!s.startsWith("conf.")) {
                        newSet.add(s);
                    }
                }
                return newSet;
            }
            return invalidSettings;
        }
    }

    private static class TypeSettings {
        private static final Set<String> GENERIC = ImmutableSet.of("max_restore_bytes_per_sec", "max_snapshot_bytes_per_sec");
        private final Set<String> required;
        private final Set<String> all;

        public TypeSettings(Set<String> required, Set<String> optional) {
            this.required = required;
            this.all = ImmutableSet.<String>builder().addAll(required).addAll(optional).addAll(GENERIC).build();
        }
    }
}
