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

package io.crate.execution.engine.indexing;

import com.google.common.collect.ImmutableMap;
import io.crate.data.Row;
import io.crate.data.RowN;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class UpsertResults {

    private final Map<String, Result> resultsByUri = new HashMap<>(1);
    private final ImmutableMap<String, String> nodeInfo;

    UpsertResults() {
        this.nodeInfo = null;
    }

    UpsertResults(ImmutableMap<String, String> nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    void addResult(long successRowCount) {
        Result result = getResultSafe(null);
        result.successRowCount += successRowCount;
    }

    void addResult(String uri, @Nullable String failureMessage, long lineNumber) {
        Result result = getResultSafe(uri);
        if (failureMessage == null) {
            result.successRowCount += 1;
        } else {
            result.errorRowCount += 1;
            result.updateErrorCount(failureMessage, Collections.singletonList(lineNumber), 1L);
        }
    }

    void addUriFailure(String uri, String uriFailure) {
        assert uri != null : "expecting URI argument not to be null";
        Result result = getResultSafe(uri);
        result.sourceUriFailure = true;
        result.updateErrorCount(uriFailure, Collections.emptyList(), 1L);
    }

    private Result getResultSafe(@Nullable String uri) {
        Result result = resultsByUri.get(uri);
        if (result == null) {
            result = new Result();
            resultsByUri.put(uri, result);
        }
        return result;
    }

    long getSuccessRowCountForNoUri() {
        return getResultSafe(null).successRowCount;
    }

    void merge(UpsertResults other) {
        for (Map.Entry<String, Result> entry : other.resultsByUri.entrySet()) {
            Result result = resultsByUri.get(entry.getKey());
            if (result == null) {
                resultsByUri.put(entry.getKey(), entry.getValue());
            } else {
                result.merge(entry.getValue());
            }

        }
    }

    Iterable<Row> rowsIterable() {
        Stream<Row> s = resultsByUri.entrySet().stream()
            .sorted(Comparator.comparing(Map.Entry::getKey))
            .map(e -> {
                Result r = e.getValue();
                if (r.sourceUriFailure) {
                    return new RowN(new Object[]{nodeInfo, e.getKey(), null, null, r.errors});
                } else {
                    return new RowN(new Object[]{nodeInfo, e.getKey(), r.successRowCount, r.errorRowCount, r.errors});
                }
            });
        return s::iterator;
    }

    static class Result {

        private static final String ERROR_COUNT_KEY = "count";
        private static final String LINE_NUMBERS_KEY = "line_numbers";

        private long successRowCount = 0;
        private long errorRowCount = 0;
        private final Map<String, Map<String, Object>> errors = new HashMap<>();
        private boolean sourceUriFailure = false;

        void merge(Result upsertResult) {
            successRowCount += upsertResult.successRowCount;
            errorRowCount += upsertResult.errorRowCount;
            if (sourceUriFailure == false) {
                sourceUriFailure = upsertResult.sourceUriFailure;
            }
            for (Map.Entry<String, Map<String, Object>> entry : upsertResult.errors.entrySet()) {
                Map<String, Object> val = entry.getValue();
                //noinspection unchecked
                List<Long> lineNumbers = (List<Long>) val.get(LINE_NUMBERS_KEY);
                Long errorCnt = (Long) val.get(ERROR_COUNT_KEY);
                updateErrorCount(entry.getKey(), lineNumbers, errorCnt);
            }
        }

        private void updateErrorCount(String msg, List<Long> lineNumbers, Long increaseBy) {
            Map<String, Object> errorEntry = errors.get(msg);
            Long cnt = 0L;
            List<Long> currentLineNumbers;
            if (errorEntry == null) {
                errorEntry = new HashMap<>(1);
                errors.put(msg, errorEntry);
                currentLineNumbers = new ArrayList<>(lineNumbers);
            } else {
                cnt = (Long) errorEntry.get(ERROR_COUNT_KEY);
                //noinspection unchecked
                currentLineNumbers = (List<Long>) errorEntry.get(LINE_NUMBERS_KEY);
                currentLineNumbers.addAll(lineNumbers);
            }
            errorEntry.put(ERROR_COUNT_KEY, cnt + increaseBy);
            errorEntry.put(LINE_NUMBERS_KEY, currentLineNumbers);
        }
    }
}
