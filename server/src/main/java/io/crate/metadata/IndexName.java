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

package io.crate.metadata;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import io.crate.blob.v2.BlobIndex;
import io.crate.common.StringUtils;
import io.crate.common.collections.Lists;
import io.crate.data.Input;
import io.crate.metadata.blob.BlobSchemaInfo;

public final class IndexName {

    private static final Logger LOGGER = LogManager.getLogger(IndexName.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LOGGER);

    public static final int MAX_INDEX_NAME_BYTES = 255;

    private static final String PARTITIONED_KEY_WORD = "partitioned";

    @VisibleForTesting
    static final String PARTITIONED_TABLE_PART = "." + PARTITIONED_KEY_WORD + ".";

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public static void validate(String indexName) {
        validate(indexName, InvalidIndexNameException::new);
    }

    /**
     * Validate the name for an index or alias against some static rules.
     */
    public static void validate(String indexName,
                                BiFunction<String, String, ? extends RuntimeException> exceptionCtor) {
        if (!Strings.validFileName(indexName)) {
            throw exceptionCtor.apply(indexName, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
        if (indexName.contains("#")) {
            throw exceptionCtor.apply(indexName, "must not contain '#'");
        }
        if (indexName.contains(":")) {
            DEPRECATION_LOGGER.deprecatedAndMaybeLog("index_name_contains_colon",
                "index or alias name [" + indexName + "] containing ':' is deprecated. CrateDB 4.x will read, " +
                    "but not allow creation of new indices containing ':'");
        }
        if (indexName.charAt(0) == '-' || indexName.charAt(0) == '+') {
            throw exceptionCtor.apply(indexName, "must not start with '-', or '+'");
        }
        int byteCount = 0;
        try {
            byteCount = indexName.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            throw new ElasticsearchException("Unable to determine length of index name", e);
        }
        if (byteCount > MAX_INDEX_NAME_BYTES) {
            throw exceptionCtor.apply(indexName, "index name is too long, (" + byteCount + " > " + MAX_INDEX_NAME_BYTES + ")");
        }
        if (indexName.equals(".") || indexName.equals("..")) {
            throw exceptionCtor.apply(indexName, "must not be '.' or '..'");
        }
    }

    /**
     * Parses an encoded indexName into its parts. Supported Formats:
     *
     * <ul>
     *  <li>table_name</li>
     *  <li>schema.table_name</li>
     *  <li>.partitioned.table_name.[ident]</li>
     *  <li>schema..partitioned.table_name.[ident]</li>
     * </ul>
     **/
    public static IndexParts decode(String indexName) {
        if (BlobIndex.isBlobIndex(indexName)) {
            return new IndexParts(BlobSchemaInfo.NAME, BlobIndex.stripPrefix(indexName), null);
        }
        // Index names are only allowed to contain '.' as separators
        String[] parts = indexName.split("\\.", 6);
        return switch (parts.length) {
            // "table_name"
            case 1 -> new IndexParts(Schemas.DOC_SCHEMA_NAME, indexName, null);

            // "schema"."table_name"
            case 2 -> new IndexParts(parts[0], parts[1], null);

            // ""."partitioned"."table_name". ["ident"]
            case 4 -> {
                assertEmpty(parts[0]);
                assertPartitionPrefix(parts[1]);
                yield new IndexParts(Schemas.DOC_SCHEMA_NAME, parts[2], parts[3]);
            }

            // "schema".""."partitioned"."table_name". ["ident"]
            case 5 -> {
                assertEmpty(parts[1]);
                assertPartitionPrefix(parts[2]);
                yield new IndexParts(parts[0], parts[3], parts[4]);
            }
            default -> throw new IllegalArgumentException("Invalid index name: " + indexName);
        };
    }

    public static String encode(RelationName relationName, String partitionIdent) {
        return encode(relationName.schema(), relationName.name(), partitionIdent);
    }

    /**
     * Encodes the given parts to a CrateDB index name.
     */
    public static String encode(String schema, String table, @Nullable String partitionIdent) {
        if (partitionIdent == null) {
            if (schema.equals(Schemas.DOC_SCHEMA_NAME)) {
                return table;
            }
            return schema + "." + table;
        }
        if (schema.equals(Schemas.DOC_SCHEMA_NAME)) {
            return IndexName.PARTITIONED_TABLE_PART + table + "." + partitionIdent;
        }
        return schema + "." + IndexName.PARTITIONED_TABLE_PART + table + "." + partitionIdent;
    }

    /**
     * Checks whether the index/template name belongs to a partitioned table.
     *
     * A partition index name looks like on of these:
     *
     * .partitioned.table.ident
     * schema..partitioned.table.ident
     * schema..partitioned.table.
     *
     * @param templateOrIndex The index name to check
     * @return True if the index/template name denotes a partitioned table
     */
    public static boolean isPartitioned(String templateOrIndex) {
        int idx1 = templateOrIndex.indexOf('.');
        if (idx1 == -1) {
            return false;
        }
        int idx2 = templateOrIndex.indexOf(PARTITIONED_TABLE_PART, idx1);
        if (idx2 == -1) {
            return false;
        }
        int diff = idx2 - idx1;
        return ((diff == 0 && idx1 == 0) || diff == 1) && idx2 + PARTITIONED_TABLE_PART.length() < templateOrIndex.length();
    }

    public static boolean isDangling(String indexName) {
        return indexName.startsWith(".") &&
               !indexName.startsWith(PARTITIONED_TABLE_PART) &&
               !BlobIndex.isBlobIndex(indexName);
    }

    public static Supplier<String> createResolver(RelationName relationName,
                                                  @Nullable String partitionIdent,
                                                  @Nullable List<Input<?>> partitionedByInputs) {
        if (partitionIdent == null && (partitionedByInputs == null || partitionedByInputs.isEmpty())) {
            return createResolver(relationName);
        }
        if (partitionIdent == null) {
            final RelationName relationName1 = relationName;
            final List<Input<?>> partitionedByInputs1 = partitionedByInputs;
            assert partitionedByInputs1.size() > 0 : "must have at least 1 partitionedByInput";
            final LoadingCache<List<String>, String> cache = Caffeine.newBuilder()
                .executor(Runnable::run)
                .initialCapacity(10)
                .maximumSize(20)
                .build(new CacheLoader<List<String>, String>() {
                    @Override
                    public String load(@NotNull List<String> key) {
                        return IndexName.encode(relationName1, PartitionName.encodeIdent(key));
                    }
                });
            return () -> {
                // copy because the values of the inputs are mutable
                List<String> partitions = Lists.map(partitionedByInputs1, input -> StringUtils.nullOrString(input.value()));
                return cache.get(partitions);
            };
        }
        String indexName = IndexName.encode(relationName, partitionIdent);
        return () -> indexName;
    }

    public static Supplier<String> createResolver(final RelationName relationName) {
        String indexNameOrAlias = relationName.indexNameOrAlias();
        return () -> indexNameOrAlias;
    }

    private static void assertPartitionPrefix(String prefix) {
        if (!PARTITIONED_KEY_WORD.equals(prefix)) {
            throw new IllegalArgumentException("Invalid partition prefix: " + prefix);
        }
    }

    private static void assertEmpty(String prefix) {
        if (!"".equals(prefix)) {
            throw new IllegalArgumentException("Invalid index name: " + prefix);
        }
    }
}
