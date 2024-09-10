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

import java.util.function.Function;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.indices.InvalidAliasNameException;


/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 */
public final class AliasValidator {

    private AliasValidator() {
    }

    /**
     * Allows to validate an {@link org.elasticsearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public static void validateAlias(Alias alias, String index, Metadata metadata) {
        String aliasName = alias.name();
        validateAliasName(aliasName);
        if (!Strings.hasText(index)) {
            throw new IllegalArgumentException("index name is required");
        }
        IndexMetadata indexNamedSameAsAlias = ((Function<String, IndexMetadata>) metadata::index).apply(aliasName);
        if (indexNamedSameAsAlias != null) {
            throw new InvalidAliasNameException(indexNamedSameAsAlias.getIndex(), aliasName, "an index exists with the same name as the alias");
        }
    }

    /**
     * Allows to partially validate an alias, without knowing which index it'll get applied to.
     * Useful with index templates containing aliases. Checks also that it is possible to parse
     * the alias filter via {@link org.elasticsearch.common.xcontent.XContentParser},
     * without validating it as a filter though.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public static void validateAliasName(String alias) {
        if (!Strings.hasText(alias)) {
            throw new IllegalArgumentException("alias name is required");
        }
        MetadataIndexService.validateIndexOrAliasName(alias, InvalidAliasNameException::new);
    }
}
