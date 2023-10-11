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

package io.crate.planner.node.ddl;

import static io.crate.metadata.FulltextAnalyzerResolver.encodeSettings;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.CHAR_FILTER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKENIZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKEN_FILTER;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.AnalyzedCreateAnalyzer;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Tuple;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.GenericProperties;

public class CreateAnalyzerPlan implements Plan {

    private final AnalyzedCreateAnalyzer createAnalyzer;

    public CreateAnalyzerPlan(AnalyzedCreateAnalyzer createAnalyzer) {
        this.createAnalyzer = createAnalyzer;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        ClusterUpdateSettingsRequest request = createRequest(
            createAnalyzer,
            plannerContext.transactionContext(),
            dependencies.nodeContext(),
            params,
            subQueryResults,
            dependencies.fulltextAnalyzerResolver());

        dependencies.client().execute(ClusterUpdateSettingsAction.INSTANCE, request)
            .whenComplete(new OneRowActionListener<>(consumer, r -> new Row1(1L)));
    }

    @VisibleForTesting
    public static ClusterUpdateSettingsRequest createRequest(AnalyzedCreateAnalyzer createAnalyzer,
                                                             CoordinatorTxnCtx txnCtx,
                                                             NodeContext nodeCtx,
                                                             Row parameters,
                                                             SubQueryResults subQueryResults,
                                                             FulltextAnalyzerResolver ftResolver) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            parameters,
            subQueryResults
        );

        var analyzerIdent = createAnalyzer.ident();

        var tokenizer = bindTokenizer(
            createAnalyzer.tokenizer(),
            analyzerIdent,
            eval,
            ftResolver);

        var tokenFilters = bindTokenFilters(
            createAnalyzer.tokenFilters(),
            analyzerIdent,
            eval,
            ftResolver);

        var charFilters = bindCharFilters(
            createAnalyzer.charFilters(),
            analyzerIdent,
            eval,
            ftResolver);

        var genericAnalyzerSettings = bindGenericAnalyzerProperties(
            createAnalyzer.genericAnalyzerProperties(),
            analyzerIdent,
            eval);

        var analyzerSettings = buildAnalyzerSettings(
            analyzerIdent,
            createAnalyzer.extendedAnalyzerName(),
            tokenizer,
            tokenFilters,
            charFilters,
            genericAnalyzerSettings,
            ftResolver);

        Settings.Builder encodedSettingsBuilder = Settings.builder();
        encodedSettingsBuilder.put(
            ANALYZER.buildSettingName(analyzerIdent),
            encodeSettings(analyzerSettings).utf8ToString()
        );
        if (tokenizer != null && !tokenizer.v2().isEmpty()) {
            encodedSettingsBuilder.put(
                TOKENIZER.buildSettingName(tokenizer.v1()),
                encodeSettings(tokenizer.v2()).utf8ToString()
            );
        }
        for (Map.Entry<String, Settings> tokenFilter : tokenFilters.entrySet()) {
            if (!tokenFilter.getValue().isEmpty()) {
                encodedSettingsBuilder.put(
                    TOKEN_FILTER.buildSettingName(tokenFilter.getKey()),
                    encodeSettings(tokenFilter.getValue()).utf8ToString()
                );
            }
        }
        for (Map.Entry<String, Settings> charFilter : charFilters.entrySet()) {
            if (!charFilter.getValue().isEmpty()) {
                encodedSettingsBuilder.put(
                    CHAR_FILTER.buildSettingName(charFilter.getKey()),
                    encodeSettings(charFilter.getValue()).utf8ToString()
                );
            }
        }

        return new ClusterUpdateSettingsRequest()
            .persistentSettings(encodedSettingsBuilder.build());
    }

    @Nullable
    private static Tuple<String, Settings> bindTokenizer(@Nullable Tuple<String, GenericProperties<Symbol>> tokenizer,
                                                         String analyzerIdent,
                                                         Function<? super Symbol, Object> eval,
                                                         FulltextAnalyzerResolver ftResolver) {
        if (tokenizer == null) {
            return null;
        }

        var name = tokenizer.v1();
        var properties = tokenizer.v2();

        if (properties.isEmpty()) {
            // tokenizerProperties a builtin tokenizer without parameters
            if (!ftResolver.hasBuiltInTokenizer(name)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Non-existing tokenizer '%s'", name));
            }
            return new Tuple<>(name, Settings.EMPTY);
        } else {
            if (!ftResolver.hasBuiltInTokenizer(name)) {
                // type mandatory
                String evaluatedType = extractType(properties, eval);
                if (!ftResolver.hasBuiltInTokenizer(evaluatedType)) {
                    // only builtin tokenizers can be extended, for now
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Non-existing built-in tokenizer type '%s'", evaluatedType));
                }
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "tokenizer name '%s' is reserved", name));
            }

            // transform name as tokenizer is not publicly available
            name = String.format(Locale.ENGLISH, "%s_%s", analyzerIdent, name);
            Settings.Builder builder = Settings.builder();
            for (Map.Entry<String, Symbol> property : properties.properties().entrySet()) {
                String settingName = TOKENIZER.buildSettingChildName(name, property.getKey());
                builder.putStringOrList(settingName, eval.apply(property.getValue()));
            }
            return new Tuple<>(name, builder.build());
        }
    }

    private static Map<String, Settings> bindTokenFilters(Map<String, GenericProperties<Symbol>> tokenFilters,
                                                          String analyzerIdent,
                                                          Function<? super Symbol, Object> eval,
                                                          FulltextAnalyzerResolver ftResolver) {
        HashMap<String, Settings> boundTokenFilters = new HashMap<>(tokenFilters.size());

        for (Map.Entry<String, GenericProperties<Symbol>> tokenFilter : tokenFilters.entrySet()) {
            var name = tokenFilter.getKey();
            var properties = tokenFilter.getValue();

            if (properties.isEmpty()) {
                // use a builtin token-filter without parameters
                // validate
                if (!ftResolver.hasBuiltInTokenFilter(name)) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH, "Non-existing built-in token-filter '%s'", name));
                }
                boundTokenFilters.put(name, Settings.EMPTY);
            } else {
                Settings.Builder builder = Settings.builder();
                // transform name as token-filter is not publicly available
                String fullName = analyzerIdent + '_' + name;
                if (!ftResolver.hasBuiltInTokenFilter(name)) {
                    // type mandatory when name is not a builtin filter
                    String evaluatedType = extractType(properties, eval);
                    if (!ftResolver.hasBuiltInTokenFilter(evaluatedType)) {
                        // only builtin token-filters can be extended, for now
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "Non-existing built-in token-filter type '%s'", evaluatedType));
                    }
                } else {
                    if (properties.get("type") != null) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "token-filter name '%s' is reserved, 'type' property forbidden here", name));
                    }

                    builder.put(TOKEN_FILTER.buildSettingChildName(fullName, "type"), name);
                }

                for (Map.Entry<String, Symbol> property : properties.properties().entrySet()) {
                    String settingName = TOKEN_FILTER.buildSettingChildName(fullName, property.getKey());
                    builder.putStringOrList(settingName, eval.apply(property.getValue()));
                }
                boundTokenFilters.put(fullName, builder.build());
            }
        }
        return boundTokenFilters;
    }

    private static Map<String, Settings> bindCharFilters(Map<String, GenericProperties<Symbol>> charFilters,
                                                         String analyzerIdent,
                                                         Function<? super Symbol, Object> eval,
                                                         FulltextAnalyzerResolver ftResolver) {
        HashMap<String, Settings> boundedCharFilters = new HashMap<>(charFilters.size());

        for (Map.Entry<String, GenericProperties<Symbol>> charFilter : charFilters.entrySet()) {
            var name = charFilter.getKey();
            var properties = charFilter.getValue();

            // use a builtin char-filter without parameters
            if (properties.isEmpty()) {
                if (!ftResolver.hasBuiltInCharFilter(name)) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Non-existing built-in char-filter '%s'", name));
                }
                validateCharFilterProperties(name, properties);
                boundedCharFilters.put(name, Settings.EMPTY);
            } else {
                String type = extractType(properties, eval);
                if (!ftResolver.hasBuiltInCharFilter(type)) {
                    // only builtin char-filters can be extended, for now
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH, "Non-existing built-in char-filter type '%s'", type));
                }
                validateCharFilterProperties(type, properties);

                // transform name as char-filter is not publicly available
                name = String.format(Locale.ENGLISH, "%s_%s", analyzerIdent, name);
                Settings.Builder builder = Settings.builder();
                for (Map.Entry<String, Symbol> charFilterProperty : properties.properties().entrySet()) {
                    String settingName = CHAR_FILTER.buildSettingChildName(name, charFilterProperty.getKey());
                    builder.putStringOrList(settingName, eval.apply(charFilterProperty.getValue()));
                }
                boundedCharFilters.put(name, builder.build());
            }
        }
        return boundedCharFilters;
    }

    private static Settings bindGenericAnalyzerProperties(GenericProperties<Symbol> properties,
                                                          String analyzerIdent,
                                                          Function<? super Symbol, Object> eval) {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Symbol> property : properties.properties().entrySet()) {
            String settingName = ANALYZER.buildSettingChildName(analyzerIdent, property.getKey());
            builder.putStringOrList(settingName, eval.apply(property.getValue()));
        }
        return builder.build();
    }

    private static String extractType(GenericProperties<Symbol> properties,
                                      Function<? super Symbol, Object> eval) {
        Symbol type = properties.get("type");
        if (type == null) {
            throw new IllegalArgumentException("'type' property missing");
        }
        return (String) eval.apply(type);
    }

    private static void validateCharFilterProperties(String type, GenericProperties properties) {
        if (properties.isEmpty() && !type.equals("html_strip")) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "CHAR_FILTER of type '%s' needs additional parameters", type));
        }
    }

    /**
     * Create analyzer settings - possibly referencing charFilters,
     * tokenFilters, and tokenizers.
     *
     * @return Settings describing a custom or extended builtin-analyzer.
     */
    private static Settings buildAnalyzerSettings(String analyzerIdent,
                                                  String extendedAnalyzerName,
                                                  Tuple<String, Settings> tokenizer,
                                                  Map<String, Settings> tokenFilters,
                                                  Map<String, Settings> charFilters,
                                                  Settings genericAnalyzerSettings,
                                                  FulltextAnalyzerResolver ftResolver) {
        // resolve custom analyzer settings, if any
        var extendedCustomAnalyzerSettings = ftResolver.getCustomAnalyzer(extendedAnalyzerName);

        Settings.Builder builder = Settings.builder();

        boolean extendsCustomAnalyzer =
            extendedAnalyzerName != null
            && extendedCustomAnalyzerSettings != null
            && extendedCustomAnalyzerSettings
                .get(ANALYZER.buildSettingChildName(extendedAnalyzerName, "type")).equals("custom");

        if (extendsCustomAnalyzer) {
            // use analyzer-settings from extended analyzer only
            Settings stripped = extendedCustomAnalyzerSettings.getByPrefix(
                ANALYZER.buildSettingChildNamePrefix(extendedAnalyzerName));

            for (String settingName : stripped.keySet()) {
                builder.put(
                    ANALYZER.buildSettingChildName(analyzerIdent, settingName),
                    stripped.get(settingName)
                );
            }

            if (tokenizer == null) {
                // set tokenizer if not defined in extending analyzer
                String extendedTokenizerName = extendedCustomAnalyzerSettings.get(
                    ANALYZER.buildSettingChildName(extendedAnalyzerName, TOKENIZER.getName()));
                if (extendedTokenizerName != null) {
                    Settings extendedTokenizerSettings = ftResolver.getCustomTokenizer(
                        extendedTokenizerName);
                    if (extendedTokenizerSettings != null) {
                        tokenizer = new Tuple<>(extendedTokenizerName, extendedTokenizerSettings);
                    } else {
                        tokenizer = new Tuple<>(extendedTokenizerName, Settings.EMPTY);
                    }
                }
            }

            if (tokenFilters.isEmpty()) {
                // only use inherited token filters if none are defined in extending analyzer
                List<String> extendedTokenFilterNames = extendedCustomAnalyzerSettings.getAsList(
                    ANALYZER.buildSettingChildName(extendedAnalyzerName, TOKEN_FILTER.getName()));
                for (String extendedTokenFilterName : extendedTokenFilterNames) {
                    Settings extendedTokenFilterSettings = ftResolver.getCustomTokenFilter(
                        extendedTokenFilterName);
                    if (extendedTokenFilterSettings != null) {
                        tokenFilters.put(extendedTokenFilterName, extendedTokenFilterSettings);
                    } else {
                        tokenFilters.put(extendedTokenFilterName, Settings.EMPTY);
                    }
                }
            }

            if (charFilters.isEmpty()) {
                // only use inherited charfilters if none are defined in extending analyzer
                List<String> extendedCustomCharFilterNames = extendedCustomAnalyzerSettings.getAsList(
                    ANALYZER.buildSettingChildName(extendedAnalyzerName, CHAR_FILTER.getName()));
                for (String extendedCustomCharFilterName : extendedCustomCharFilterNames) {
                    Settings extendedCustomCharFilterSettings = ftResolver.getCustomCharFilter(
                        extendedCustomCharFilterName);
                    if (extendedCustomCharFilterSettings != null) {
                        charFilters.put(extendedCustomCharFilterName, extendedCustomCharFilterSettings);
                    } else {
                        charFilters.put(extendedCustomCharFilterName, Settings.EMPTY);
                    }
                }
            }

        } else if (extendsBuiltInAnalyzer(extendedAnalyzerName, extendedCustomAnalyzerSettings)) {
            // generic properties for extending builtin analyzers
            if (genericAnalyzerSettings != null) {
                builder.put(genericAnalyzerSettings);
            }
        }

        // analyzer type
        String analyzerType = "custom";
        if (extendsBuiltInAnalyzer(extendedAnalyzerName, extendedCustomAnalyzerSettings)) {
            if (extendedCustomAnalyzerSettings != null) {
                analyzerType = extendedCustomAnalyzerSettings.get(
                    ANALYZER.buildSettingChildName(extendedAnalyzerName, "type"));
            } else {
                // direct extending builtin analyzer, use name as type
                analyzerType = extendedAnalyzerName;
            }
        }
        builder.put(
            ANALYZER.buildSettingChildName(analyzerIdent, "type"),
            analyzerType
        );

        if (tokenizer != null) {
            builder.put(
                ANALYZER.buildSettingChildName(analyzerIdent, TOKENIZER.getName()),
                tokenizer.v1()
            );
        } else if (!extendsBuiltInAnalyzer(extendedAnalyzerName, extendedCustomAnalyzerSettings)) {
            throw new UnsupportedOperationException("Tokenizer missing from non-extended analyzer");
        }
        if (charFilters.size() > 0) {
            String[] charFilterNames = charFilters.keySet().toArray(new String[0]);
            builder.putList(
                ANALYZER.buildSettingChildName(analyzerIdent, CHAR_FILTER.getName()),
                charFilterNames
            );
        }
        if (tokenFilters.size() > 0) {
            String[] tokenFilterNames = tokenFilters.keySet().toArray(new String[0]);
            builder.putList(
                ANALYZER.buildSettingChildName(analyzerIdent, TOKEN_FILTER.getName()),
                tokenFilterNames
            );
        }
        return builder.build();
    }

    private static boolean extendsBuiltInAnalyzer(String extendedAnalyzerName,
                                                  Settings extendedCustomAnalyzerSettings) {
        return extendedAnalyzerName != null
               && (extendedCustomAnalyzerSettings == null || !extendedCustomAnalyzerSettings
            .get(ANALYZER.buildSettingChildName(extendedAnalyzerName, "type")).equals("custom"));
    }
}
