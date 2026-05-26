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

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
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

    /**
     * Validates the provided {@code settings} for the given repository {@code type}.
     * <p>
     * Ensures that only supported keys are present and that no required properties are missing.
     * Throws an exception if either condition is violated.
     *
     * @param type     the repository type used to determine supported and required settings
     * @param settings the settings to validate
     * @throws IllegalArgumentException if unsupported keys are present or required settings are missing
     */
    public void validate(String type, Settings settings) {
        // make sure only supported keys are present
        validateSupportedOnly(type, new GenericProperties<>(settings.getAsStructuredMap()));

        // make sure no required properties are missing
        TypeSettings typeSettings = settingsForType(type);
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

    /**
     * Validates that the provided {@code properties} contain only supported keys for the given {@code type}.
     * The method is useful in contexts where an object is updated. To check if required properties are present
     * use {@link #validate(String, Settings)}.
     *
     * @param type          the type whose supported keys are checked against
     * @param properties    the properties map to validate
     * @throws IllegalArgumentException if unsupported properties are found
     */
    public void validateSupportedOnly(String type, GenericProperties<Object> properties) {
        var supportedKeys = settingsForType(type)
            .all()
            .keySet();
        properties.ensureContainsOnly(supportedKeys);
    }

    public TypeSettings settingsForType(String type) {
        TypeSettings typeSettings = this.typeSettings.get(type);
        if (typeSettings == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid repository type \"%s\"", type));
        }

        return typeSettings;
    }
}
