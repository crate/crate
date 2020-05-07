/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.crate.expression.udf.UserDefinedFunctionsMetaData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType;
import static java.util.Collections.emptyIterator;

public class RoutineInfos implements Iterable<RoutineInfo> {

    private static final Logger LOGGER = LogManager.getLogger(RoutineInfos.class);
    private final UserDefinedFunctionsMetaData functionsMetaData;
    private FulltextAnalyzerResolver ftResolver;

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

    public RoutineInfos(FulltextAnalyzerResolver ftResolver, UserDefinedFunctionsMetaData functionsMetaData) {
        this.ftResolver = ftResolver;
        this.functionsMetaData = functionsMetaData;
    }

    private Iterator<RoutineInfo> builtInAnalyzers() {
        return Iterators.transform(
            ftResolver.getBuiltInAnalyzers().iterator(),
            new Function<String, RoutineInfo>() {
                @Nullable
                @Override
                public RoutineInfo apply(@Nullable String input) {
                    return new RoutineInfo(input,
                        RoutineType.ANALYZER.getName());
                }
            });
    }

    private Iterator<RoutineInfo> builtInCharFilters() {
        return Iterators.transform(
            ftResolver.getBuiltInCharFilters().iterator(),
            new Function<String, RoutineInfo>() {
                @Nullable
                @Override
                public RoutineInfo apply(@Nullable String input) {
                    return new RoutineInfo(input,
                        RoutineType.CHAR_FILTER.getName());
                }
            });
    }

    private Iterator<RoutineInfo> builtInTokenFilters() {
        return Iterators.transform(
            ftResolver.getBuiltInTokenFilters().iterator(),
            new Function<String, RoutineInfo>() {
                @Nullable
                @Override
                public RoutineInfo apply(@Nullable String input) {
                    return new RoutineInfo(input,
                        RoutineType.TOKEN_FILTER.getName());
                }
            });
    }

    private Iterator<RoutineInfo> builtInTokenizers() {
        return Iterators.transform(
            ftResolver.getBuiltInTokenizers().iterator(),
            new Function<String, RoutineInfo>() {
                @Nullable
                @Override
                public RoutineInfo apply(@Nullable String input) {
                    return new RoutineInfo(input,
                        RoutineType.TOKENIZER.getName());
                }
            });
    }

    private Iterator<RoutineInfo> userDefinedFunctions() {
        if (functionsMetaData == null) {
            return emptyIterator();
        }
        return Iterators.transform(functionsMetaData.functionsMetaData().iterator(),
            input -> new RoutineInfo(
                input.name(),
                RoutineType.FUNCTION.getName(),
                input.schema(),
                input.specificName(),
                input.definition(),
                input.language(),
                input.returnType().getName(),
                true)
        );
    }

    private Iterator<RoutineInfo> customIterators() {
        try {
            Iterator<RoutineInfo> cAnalyzersIterator = Iterators.transform(
                ftResolver.getCustomAnalyzers().entrySet().iterator(),
                input -> new RoutineInfo(
                    input.getKey(),
                    RoutineType.ANALYZER.getName(),
                    routineSettingsToDefinition(input.getKey(), input.getValue(), RoutineType.ANALYZER)));
            Iterator<RoutineInfo> cCharFiltersIterator = Iterators.transform(
                ftResolver.getCustomCharFilters().entrySet().iterator(),
                input -> new RoutineInfo(
                    input.getKey(),
                    RoutineType.CHAR_FILTER.getName(),
                    routineSettingsToDefinition(input.getKey(), input.getValue(), RoutineType.CHAR_FILTER)));
            Iterator<RoutineInfo> cTokenFiltersIterator = Iterators.transform(
                ftResolver.getCustomTokenFilters().entrySet().iterator(),
                input -> new RoutineInfo(
                    input.getKey(),
                    RoutineType.TOKEN_FILTER.getName(),
                    routineSettingsToDefinition(input.getKey(), input.getValue(), RoutineType.TOKEN_FILTER)));
            Iterator<RoutineInfo> cTokenizersIterator = Iterators.transform(
                ftResolver.getCustomTokenizers().entrySet().iterator(),
                input -> new RoutineInfo(
                    input.getKey(),
                    RoutineType.TOKENIZER.getName(),
                    routineSettingsToDefinition(input.getKey(), input.getValue(), RoutineType.TOKENIZER)));
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
