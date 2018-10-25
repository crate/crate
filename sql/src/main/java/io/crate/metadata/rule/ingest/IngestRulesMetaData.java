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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class IngestRulesMetaData extends AbstractNamedDiffable<MetaData.Custom> implements MetaData.Custom {

    public static final String TYPE = "ingest_rules";

    /**
     * Returns a copy of {@link IngestRulesMetaData}
     */
    public static IngestRulesMetaData copyOf(@Nullable IngestRulesMetaData oldMetaData) {
        if (oldMetaData == null) {
            return new IngestRulesMetaData();
        }

        Map<String, Set<IngestRule>> copyIngestRules = new HashMap<>(oldMetaData.sourceIngestRules.size());
        for (Map.Entry<String, Set<IngestRule>> entry : oldMetaData.sourceIngestRules.entrySet()) {
            copyIngestRules.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return new IngestRulesMetaData(copyIngestRules);
    }

    /**
     * Returns a copy of the {@link IngestRulesMetaData} including a copied list of rules if at least one
     * rule was replaced. Otherwise returns NULL to indicate that nothing was changed.
     * Rules whose target table idents are matching the given source ident are replaced by a copy where the ident
     * is changed to the given target ident.
     */
    @Nullable
    public static IngestRulesMetaData maybeCopyAndReplaceTargetTableIdents(IngestRulesMetaData oldMetaData,
                                                                           String sourceIdent,
                                                                           String targetIdent) {
        boolean rulesChanged = false;
        Map<String, Set<IngestRule>> sourceIngestRules = new HashMap<>(oldMetaData.sourceIngestRules.size());
        for (Map.Entry<String, Set<IngestRule>> entry : oldMetaData.sourceIngestRules.entrySet()) {
            Set<IngestRule> ingestRules = new HashSet<>(entry.getValue().size());
            for (IngestRule rule : entry.getValue()) {
                if (rule.getTargetTable().equals(sourceIdent)) {
                    IngestRule newRule = new IngestRule(rule.getName(), targetIdent, rule.getCondition());
                    ingestRules.add(newRule);
                    rulesChanged = true;
                } else {
                    ingestRules.add(rule);
                }
            }
            sourceIngestRules.put(entry.getKey(), ingestRules);
        }

        if (rulesChanged) {
            return new IngestRulesMetaData(sourceIngestRules);
        }
        return null;
    }

    private final Map<String, Set<IngestRule>> sourceIngestRules;

    private IngestRulesMetaData() {
        this(new HashMap<>());
    }

    public IngestRulesMetaData(Map<String, Set<IngestRule>> sourceIngestRules) {
        this.sourceIngestRules = sourceIngestRules;
    }

    public IngestRulesMetaData(StreamInput in) throws IOException {
        int size = in.readVInt();
        sourceIngestRules = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String source = in.readString();
            int numRules = in.readVInt();
            Set<IngestRule> sourceRules = new HashSet<>(numRules);
            for (int j = 0; j < numRules; j++) {
                sourceRules.add(new IngestRule(in));
            }
            sourceIngestRules.put(source, sourceRules);
        }
    }

    @Nullable
    Set<IngestRule> getIngestRules(String sourceIdent) {
        return sourceIngestRules.get(sourceIdent);
    }

    @Nullable
    public Map<String, Set<IngestRule>> getIngestRules() {
        return sourceIngestRules;
    }

    public void createIngestRule(String sourceIdent, IngestRule ingestRule) throws IllegalArgumentException {
        for (Set<IngestRule> ingestRules : sourceIngestRules.values()) {
            ingestRules.forEach(r -> {
                if (r.getName().equals(ingestRule.getName())) {
                    throw new IllegalArgumentException("Ingest rule with name " + ingestRule.getName() + " already exists");
                }
            });
        }

        Set<IngestRule> ingestRules = sourceIngestRules.computeIfAbsent(sourceIdent, k -> new HashSet<>());
        ingestRules.add(ingestRule);
    }

    public void dropIngestRule(String ruleName) {
        for (Set<IngestRule> ingestRules : sourceIngestRules.values()) {
            Iterator<IngestRule> iterator = ingestRules.iterator();
            while (iterator.hasNext()) {
                IngestRule rule = iterator.next();
                if (rule.getName().equals(ruleName)) {
                    iterator.remove();
                    return;
                }
            }
        }

        throw new ResourceNotFoundException("Ingest rule " + ruleName + " doesn't exist");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IngestRulesMetaData that = (IngestRulesMetaData) o;
        return sourceIngestRules.equals(that.sourceIngestRules);
    }

    @Override
    public int hashCode() {
        return sourceIngestRules.hashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(sourceIngestRules.size());
        for (Map.Entry<String, Set<IngestRule>> entry : sourceIngestRules.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (IngestRule ingestRule : entry.getValue()) {
                ingestRule.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, Set<IngestRule>> entry : sourceIngestRules.entrySet()) {
            builder.startArray(entry.getKey());
            for (IngestRule ingestRule : entry.getValue()) {
                ingestRuleToXContent(ingestRule, builder);
            }
            builder.endArray();
        }

        return builder;
    }

    private void ingestRuleToXContent(IngestRule ingestRule, XContentBuilder builder) throws IOException {
        builder.startObject()
            .field("name", ingestRule.getName())
            .field("targetTable", ingestRule.getTargetTable())
            .field("condition", ingestRule.getCondition())
            .endObject();
    }

    public static IngestRulesMetaData fromXContent(XContentParser parser) throws IOException {
        IngestRulesMetaData metaData = new IngestRulesMetaData();
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            String source = parser.currentName();
            Set<IngestRule> rulesForSource = metaData.getIngestRules(source);
            if (rulesForSource == null) {
                rulesForSource = new HashSet<>();
                metaData.sourceIngestRules.put(source, rulesForSource);
            }
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token == XContentParser.Token.START_OBJECT) {
                    ingestRuleFromXContent(parser, rulesForSource);
                }
            }
        }
        return metaData;
    }

    private static void ingestRuleFromXContent(XContentParser parser, Set<IngestRule> ingestRules) throws IOException {
        String name = null;
        String targetTable = null;
        String condition = null;
        XContentParser.Token currentToken;
        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (currentToken == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                parser.nextToken();
                switch (currentFieldName) {
                    case "name":
                        name = parser.text();
                        break;
                    case "targetTable":
                        targetTable = parser.text();
                        break;
                    case "condition":
                        condition = parser.text();
                        break;
                    default:
                        throw new ElasticsearchException("Failed to parse ingest rule");
                }
            } else if (currentToken == Token.END_ARRAY) {
                // empty list of ingest rules
                return;
            }
        }
        ingestRules.add(new IngestRule(name, targetTable, condition));
    }

    @Override
    public EnumSet<MetaData.XContentContext> context() {
        return EnumSet.of(MetaData.XContentContext.GATEWAY, MetaData.XContentContext.SNAPSHOT);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_0_0;
    }
}
