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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import io.crate.common.collections.Iterators;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.FulltextAnalyzerResolver.CustomType;

public class RoutineInfos implements Iterable<RoutineInfo> {

    private static final Logger LOGGER = LogManager.getLogger(RoutineInfos.class);
    private final FulltextAnalyzerResolver ftResolver;
    private final ClusterService clusterService;

    private enum RoutineType {
        ANALYZER(CustomType.ANALYZER.getName().toUpperCase(Locale.ENGLISH)),
        CHAR_FILTER(CustomType.CHAR_FILTER.getName().toUpperCase(Locale.ENGLISH)),
        TOKEN_FILTER("TOKEN_FILTER"),
        FUNCTION("FUNCTION"),
        TOKENIZER(CustomType.TOKENIZER.getName().toUpperCase(Locale.ENGLISH)),;
        private String name;

        RoutineType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public RoutineInfos(FulltextAnalyzerResolver ftResolver, ClusterService clusterService) {
        this.ftResolver = ftResolver;
        this.clusterService = clusterService;
    }

    private Iterator<RoutineInfo> builtInAnalyzers() {
        return ftResolver.getBuiltInAnalyzers().stream()
            .map(x -> new RoutineInfo(x, RoutineType.ANALYZER.getName()))
            .iterator();
    }

    private Iterator<RoutineInfo> builtInCharFilters() {
        return ftResolver.getBuiltInCharFilters().stream()
            .map(x -> new RoutineInfo(x, RoutineType.CHAR_FILTER.getName()))
            .iterator();
    }

    private Iterator<RoutineInfo> builtInTokenFilters() {
        return ftResolver.getBuiltInTokenFilters().stream()
            .map(x -> new RoutineInfo(x, RoutineType.TOKEN_FILTER.getName()))
            .iterator();
    }

    private Iterator<RoutineInfo> builtInTokenizers() {
        return ftResolver.getBuiltInTokenizers().stream()
            .map(x -> new RoutineInfo(x, RoutineType.TOKENIZER.getName()))
            .iterator();
    }

    private Iterator<RoutineInfo> userDefinedFunctions() {
        Metadata metadata = clusterService.state().metadata();
        UserDefinedFunctionsMetadata udf = metadata.custom(UserDefinedFunctionsMetadata.TYPE);
        if (udf == null) {
            return Collections.emptyIterator();
        }
        return udf.functionsMetadata().stream()
            .map(x -> new RoutineInfo(
                        x.name(),
                        RoutineType.FUNCTION.getName(),
                        x.schema(),
                        x.specificName(),
                        x.definition(),
                        x.language(),
                        x.returnType().getName(),
                        true)
            )
            .iterator();
    }

    private Iterator<RoutineInfo> customIterators() {
        try {
            Iterator<RoutineInfo> cAnalyzersIterator = ftResolver.getCustomAnalyzers().entrySet().stream()
                .map(x -> new RoutineInfo(
                    x.getKey(),
                    RoutineType.ANALYZER.getName(),
                    routineSettingsToDefinition(x.getKey(), x.getValue(), RoutineType.ANALYZER)
                ))
                .iterator();
            Iterator<RoutineInfo> cCharFiltersIterator = ftResolver.getCustomCharFilters().entrySet().stream()
                .map(x -> new RoutineInfo(
                    x.getKey(),
                    RoutineType.CHAR_FILTER.getName(),
                    routineSettingsToDefinition(x.getKey(), x.getValue(), RoutineType.CHAR_FILTER)
                ))
                .iterator();
            Iterator<RoutineInfo> cTokenFiltersIterator = ftResolver.getCustomTokenFilters().entrySet().stream()
                .map(x -> new RoutineInfo(
                    x.getKey(),
                    RoutineType.TOKEN_FILTER.getName(),
                    routineSettingsToDefinition(x.getKey(), x.getValue(), RoutineType.TOKEN_FILTER)
                ))
                .iterator();
            Iterator<RoutineInfo> cTokenizersIterator = ftResolver.getCustomTokenizers().entrySet().stream()
                .map(x -> new RoutineInfo(
                    x.getKey(),
                    RoutineType.TOKENIZER.getName(),
                    routineSettingsToDefinition(x.getKey(), x.getValue(), RoutineType.TOKENIZER)
                ))
                .iterator();
            return Iterators.concat(cAnalyzersIterator, cCharFiltersIterator, cTokenFiltersIterator, cTokenizersIterator);
        } catch (IOException e) {
            LOGGER.error("Could not retrieve custom routines", e);
            return null;
        }
    }

    @Override
    public Iterator<RoutineInfo> iterator() {
        return Iterators.concat(
            builtInAnalyzers(),
            builtInCharFilters(),
            builtInTokenFilters(),
            builtInTokenizers(),
            customIterators(),
            userDefinedFunctions()
        );
    }

    private static String routineSettingsToDefinition(String name, Settings settings, RoutineType type) {
        String prefix = "index.analysis." + type.getName().toLowerCase(Locale.ENGLISH) + "." + name + ".";
        return settings.getByPrefix(prefix).toString();
    }
}
