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

package io.crate.metadata.bugfix;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Maps;
import io.crate.execution.ddl.Templates;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

/**
 * Class to fix metadata corruption caused by SWAP TABLE statements like
 *
 * <pre>
 *  ALTER CLUSTER SWAP TABLE "telegraf"."metrics_new" TO "telegraf.metrics";
 * </pre>
 *
 * <p>
 * Quoting the full name ("telegraf.metrics") instead of schema and table
 * separate ("telegraf"."metrics") corrupted the metadata. It assumed
 * "telegraf.metrics" is the table name, and used "doc" (or current schema from
 * search path) as schema. A `.` is used in the template and index name to encode schema, table
 * and partition name. Having a `.` within the table name part broke the
 * encoding.
 *
 * <h2>Valid encoding schemes</h2>
 *
 * <ul>
 *  <li>{@code <table_name>}</li>
 *  <li>{@code <schema>.<table_name>}</li>
 *  <li>{@code .partitioned.<table_name>.[<ident>]}</li>
 *  <li>{@code <schema>..partitioned.<table_name>.[<ident>]}</li>
 * </ul>
 *
 * With "metrics" as table name:
 *
 * <ul>
 *  <li>{@code metrics}</li>
 *  <li>{@code custom_schema.metrics}</li>
 *  <li>{@code .partitioned.metrics.[<ident>]}</li>
 *  <li>{@code custom_schema..partitioned.metrics.[<ident>]}</li>
 * </ul>
 *
 * With the illegal `.` in the table name:
 *
 * <ul>
 *  <li>{@code tbl.with_dots} → incidentally did the right thing, moved table to correct schema</li>
 *  <li>{@code custom_schema.tbl.with_dots} → SWAP TABLE validations already prevented this; Would cause exception in {@link IndexParts}</li>
 *  <li>{@code .partitioned.tbl.with_dots.[<ident>]} → 5 parts, causes exception in {@link IndexParts} because second part is "partitioned" instead of empty
 *  <li>{@code custom_schema..partitioned.tbl.with_dots.[<ident>]} → SWAP TABLE validations prevented this, 6 parts would cause exceptions in {@link IndexParts}</li>
 * </ul>
 *
 * Therefore, the only scenario of interest is {@code .partitioned.tbl.with_dots.[<ident>]}.
 * <p>
 * When a partitioned table such as m.t is referenced as "m.t",
 * it is treated as "doc"."m.t" leading to invalid template name, ".partitioned.m.t.".
 * <p>
 * Below lists the issues that could arise while swapping due to the invalid template name:
 * <ol>
 *  <li> The first step of the swap is to remove existing template/indices - indices are properly identified and remove while template cannot be found/removed.
 *  <p> See {@link CorruptedMetadataFixer#fixInconsistencyBetweenIndexAndTemplates} how the un-removed template is removed.
 *  <li> As part of the swap, there is a table that needs to be renamed to ".partitioned.m.t.[<ident>]" (corrupting both indices/template names)
 *  <p> See {@link CorruptedMetadataFixer#fixNameOfIndexMetadata} and {@link CorruptedMetadataFixer#fixNameOfTemplateMetadata} how the names are fixed.
 *  <li> Lastly, m.t also needs to be renamed. Since there is no template named ".partitioned.m.t.", m.t is considered non-partitioned.
 *       Although all concrete indices for m.t are identified, they would overwrite one another and the last one to be renamed remains and is converted to non-partitioned index.
 *  <p> See {@link CorruptedMetadataFixer#fixInconsistencyBetweenIndexAndTemplates} how a partitioned table is recovered using the single remaining non-partitioned index.
 * </ol>
 *
 *
 * <p>
 * See https://github.com/crate/crate/issues/13380
 * </p>
 **/
public class CorruptedMetadataFixer {

    // https://github.com/crate/crate/issues/13380
    public static Metadata fixCorruptionCausedBySwapTableBug(Metadata metadata) {
        final Metadata.Builder fixedMetadata = Metadata.builder(metadata);

        fixNameOfTemplateMetadata(metadata.templates(), fixedMetadata);

        for (var indexMetadata : metadata) {
            fixNameOfIndexMetadata(indexMetadata, fixedMetadata);
        }
        for (var indexMetadata : metadata) {
            fixInconsistencyBetweenIndexAndTemplates(indexMetadata, fixedMetadata);
        }

        return fixedMetadata.build();
    }

    /**
     * Fixes inconsistencies between indexMetadata and the corresponding indexTemplateMetadata.
     *
     *      ex) test_swap_table_partitioned_dotted_src_to_non_partitioned_dotted_target
     *      - index:
     *          "m7.t7" - not partitioned / contains column s
     *          "m7.s7" - partitioned by t
     *      - templates:
     *          "m7..partitioned.t7." - partitioned by t
     *      > scenario 1: "m7.t7" is not partitioned, "m7..partitioned.t7." can be dropped.
     *      > scenario 2: "m7.s7" is not partitioned but contains "partitioned_by" column t. To fix this, a partitioned index,
     *                    "m7.s7.0400", and a template, "m7..partitioned.s7.", will be generated.
     *
     *      After the fix the metadata looks like below:
     *      - index:
     *          "m7.t7"                   - not partitioned / contains column s
     *          "m7..partitioned.s7.0400" - partitioned by t
     *      - templates:
     *          "m7..partitioned.s7."     - partitioned by t
     */
    @VisibleForTesting
    static void fixInconsistencyBetweenIndexAndTemplates(IndexMetadata indexMetadata,
                                                         Metadata.Builder fixedMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        String[] indexParts = indexName.split("\\.");
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null && !IndexParts.isPartitioned(indexName) && indexParts.length == 2) {
            Map<String, Object> metaMap = Maps.get(mappingMetadata.sourceAsMap(), "_meta");
            if (metaMap != null) {
                List<List<String>> partitionedByColumns = Maps.get(metaMap, "partitioned_by");
                if (partitionedByColumns != null && partitionedByColumns.stream().mapToLong(List::size).sum() > 0) {
                    fixedMetadata.remove(indexName);
                    fixedMetadata.put(generatePartitionedIndexMetadata(indexName, indexMetadata, partitionedByColumns));

                    String templateName = PartitionName.templateName(indexParts[0], indexParts[1]);
                    ImmutableOpenMap.Builder<String, IndexTemplateMetadata> mapBuilder = ImmutableOpenMap.builder();
                    mapBuilder.put(
                        templateName,
                        generateTemplateIndexMetadata(indexName, indexMetadata, mappingMetadata).build());

                    fixedMetadata.removeTemplate(templateName);
                    fixedMetadata.templates(mapBuilder.build());
                } else {
                    /*
                       ex) test_swap_table_partitioned_to_non_partitioned_3
                       - index:
                           "m7.t7" - not partitioned / contains column s
                           "m7.s7" - partitioned by t
                       - templates:
                           "m7..partitioned.t7." - partitioned by t
                       > since "m7.t7" is not partitioned, "m7..partitioned.t7." can be dropped.
                    */
                    fixedMetadata.removeTemplate(PartitionName.templateName(indexParts[0], indexParts[1]));
                }
            }
        }
    }

    /**
     * Generates an indexTemplateMetadata based on the given indexMetadata.
     */
    private static IndexTemplateMetadata.Builder generateTemplateIndexMetadata(String indexName,
                                                                               IndexMetadata indexMetadata,
                                                                               MappingMetadata mappingMetadata) {
        String[] indexParts = indexName.split("\\.");

        String templateName = PartitionName.templateName(indexParts[0], indexParts[1]);
        IndexTemplateMetadata.Builder templateBuilder = new IndexTemplateMetadata.Builder(templateName);
        templateBuilder.putAlias(AliasMetadata.builder(indexName).build());
        templateBuilder.putMapping(mappingMetadata.source());
        Settings indexSettings = indexMetadata.getSettings();
        var templateCompatibleSettings =
            // these settings cause exceptions when added to templates
            indexSettings.filter(k -> !(k.equals(SETTING_INDEX_UUID) || k.equals(SETTING_CREATION_DATE)));

        templateBuilder.settings(templateCompatibleSettings);
        templateBuilder.patterns(List.of(PartitionName.templatePrefix(indexParts[0], indexParts[1])));

        return templateBuilder;
    }

    /**
     * Generates a partitioned indexMetadata based on the given indexMetadata.
     */
    private static IndexMetadata.Builder generatePartitionedIndexMetadata(String indexName,
                                                                          IndexMetadata indexMetadata,
                                                                          List<List<String>> partitionedByColumns) {
        String[] indexParts = indexName.split("\\.");

        IndexMetadata.Builder partitionedIndexMetadata = new IndexMetadata.Builder(indexMetadata);
        partitionedIndexMetadata.putAlias(AliasMetadata.builder(indexName));
        String partitionedIndexName = new PartitionName(
            new RelationName(indexParts[0], indexParts[1]),
            partitionedByColumns.stream().map(c -> (String) null).toList())
            .toString();
        partitionedIndexMetadata.index(partitionedIndexName);

        return partitionedIndexMetadata;
    }

    @VisibleForTesting
    static void fixNameOfTemplateMetadata(ImmutableOpenMap<String, IndexTemplateMetadata> templates,
                                          Metadata.Builder fixedMetadata) {
        for (ObjectObjectCursor<String, IndexTemplateMetadata> e : templates) {
            RelationName fixedRelationName = fixTemplateName(e.key);
            if (fixedRelationName != null) {
                fixedMetadata.removeTemplate(e.key);
                // in case of duplicate keys, name-fixed templates overwrite
                fixedMetadata.put(Templates.copyWithNewName(e.value, fixedRelationName).build());
            } else {
                // in case of duplicate keys, do not overwrite
                if (fixedMetadata.getTemplate(e.key) == null) {
                    fixedMetadata.put(e.value);
                }
            }
        }
    }

    @VisibleForTesting
    static RelationName fixTemplateName(@Nonnull String templateName) {
        if (templateName.startsWith(".partitioned")) {
            String[] parts = templateName.split("\\.");
            if (parts.length == 4 && parts[0].isEmpty() && parts[1].equals("partitioned")) {
                return new RelationName(parts[2], parts[3]);
            }
        }
        return null;
    }

    /**
     * Fixes index names corrupted by the bug #13380, specifically the names starting with ".partitioned".
     */
    private static void fixNameOfIndexMetadata(IndexMetadata indexMetadata, Metadata.Builder fixedMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        String fixedName = fixIndexName(indexName);
        if (fixedName != null) {
            IndexMetadata corrupted = fixedMetadata.get(indexName);
            fixedMetadata.remove(indexName);
            fixedMetadata.put(IndexMetadata.builder(corrupted).index(fixedName));
        }
    }

    @VisibleForTesting
    static String fixIndexName(@Nonnull String indexName) {
        if (indexName.startsWith(".partitioned")) {
            try {
                new IndexParts(indexName);
            } catch (IllegalArgumentException e) {
                String[] indexParts = indexName.split("\\.");
                // handles exceptions thrown by 'case 5' of IndexParts ctor only: ex) .partitioned.m5.s5.042n8sjlck -> m5..partitioned.s5.042n8sjlck
                if (indexParts.length == 5) {
                    return IndexParts.toIndexName(indexParts[2], indexParts[3], indexParts[4]);
                }
            }
        }
        return null;
    }
}
