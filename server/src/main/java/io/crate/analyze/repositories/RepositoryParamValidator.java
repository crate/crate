/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.repositories;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Sets;
import io.crate.sql.tree.GenericProperties;

@Singleton
public class RepositoryParamValidator {
    private final Map<String, TypeSettings> typeSettings;

    @Inject
    public RepositoryParamValidator(Map<String, TypeSettings> repositoryTypeSettings) {
        typeSettings = repositoryTypeSettings;
    }

    public void validate(String type, GenericProperties<?> genericProperties, Settings settings) {
        TypeSettings typeSettings = settingsForType(type);
        Map<String, Setting<?>> allSettings = typeSettings.all();

        // create string settings for all dynamic settings
        GenericProperties<?> dynamicProperties = typeSettings.dynamicProperties(genericProperties);
        if (!dynamicProperties.isEmpty()) {
            // allSettings are immutable by default, copy map
            allSettings = new HashMap<>(allSettings);
            for (String key : dynamicProperties.keys()) {
                allSettings.put(key, Setting.simpleString(key));
            }
        }

        // validate all settings
        Set<String> names = settings.keySet();
        Set<String> missingRequiredSettings = Sets.difference(typeSettings.required().keySet(), names);
        if (!missingRequiredSettings.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ENGLISH,
                    "The following required parameters are missing to create a repository of type \"%s\": [%s]",
                    type,
                    String.join(", ", missingRequiredSettings))
            );
        }
    }

    public TypeSettings settingsForType(String type) {
        TypeSettings typeSettings = this.typeSettings.get(type);
        if (typeSettings == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid repository type \"%s\"", type));
        }

        return typeSettings;
    }
}
