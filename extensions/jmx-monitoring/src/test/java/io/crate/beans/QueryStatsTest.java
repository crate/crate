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

package io.crate.beans;

import io.crate.metadata.sys.ClassifiedMetrics.Metrics;
import io.crate.metadata.sys.MetricsView;
import io.crate.planner.Plan.StatementType;
import io.crate.planner.operators.StatementClassifier.Classification;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.crate.beans.QueryStats.createMetricsMap;
import static org.assertj.core.api.Assertions.assertThat;

public class QueryStatsTest {

    private static final Classification SELECT_CLASSIFICATION = new Classification(StatementType.SELECT);
    private static final Classification UPDATE_CLASSIFICATION = new Classification(StatementType.UPDATE);
    private static final Classification DELETE_CLASSIFICATION = new Classification(StatementType.DELETE);
    private static final Classification INSERT_CLASSIFICATION = new Classification(StatementType.INSERT);
    private static final Classification DDL_CLASSIFICATION = new Classification(StatementType.DDL);
    private static final Classification COPY_CLASSIFICATION = new Classification(StatementType.COPY);

    private final List<MetricsView> metrics = List.of(
        createMetric(SELECT_CLASSIFICATION, 35),
        createMetric(SELECT_CLASSIFICATION, 35),
        createMetric(UPDATE_CLASSIFICATION, 20),
        createMetric(INSERT_CLASSIFICATION, 19),
        createMetric(DELETE_CLASSIFICATION, 5),
        createMetric(DELETE_CLASSIFICATION, 10),
        createMetric(DDL_CLASSIFICATION, 1),
        createFailedExecutionMetric(SELECT_CLASSIFICATION, 20),
        createFailedExecutionMetric(COPY_CLASSIFICATION, 0)
    );

    private MetricsView createMetric(Classification classification, long duration) {
        Metrics metrics = new Metrics(classification);
        metrics.recordValues(duration, 0);
        return metrics.createMetricsView();
    }

    private MetricsView createFailedExecutionMetric(Classification classification, long duration) {
        Metrics metrics = new Metrics(classification);
        metrics.recordFailedExecution(duration);
        return metrics.createMetricsView();
    }

    @Test
    public void testTrackedStatementTypes() {
        List<MetricsView> oneMetricForEachStatementType = new ArrayList<>();
        for (StatementType type : StatementType.values()) {
            if (type.equals(StatementType.UNDEFINED)) {
                continue;
            }
            oneMetricForEachStatementType.add(createMetric(new Classification(type), 1));
        }
        Map<StatementType, QueryStats.Metric> metricsByCommand = createMetricsMap(oneMetricForEachStatementType);

        assertThat(metricsByCommand).hasSize(7);
        assertThat(metricsByCommand.get(StatementType.SELECT)).isNotNull();
        assertThat(metricsByCommand.get(StatementType.UPDATE)).isNotNull();
        assertThat(metricsByCommand.get(StatementType.INSERT)).isNotNull();
        assertThat(metricsByCommand.get(StatementType.DELETE)).isNotNull();
        assertThat(metricsByCommand.get(StatementType.DDL)).isNotNull();
        assertThat(metricsByCommand.get(StatementType.MANAGEMENT)).isNotNull();
        assertThat(metricsByCommand.get(StatementType.COPY)).isNotNull();

        assertThat(metricsByCommand.get(StatementType.UNDEFINED)).isNull();
    }

    @Test
    public void testCreateMetricsMap() {
        Map<StatementType, QueryStats.Metric> metricsByCommand = createMetricsMap(metrics);
        assertThat(metricsByCommand).hasSize(6);

        assertThat(metricsByCommand.get(StatementType.SELECT).totalCount()).isEqualTo(3L);
        assertThat(metricsByCommand.get(StatementType.SELECT).failedCount()).isEqualTo(1L);
        assertThat(metricsByCommand.get(StatementType.SELECT).sumOfDurations()).isEqualTo(90L);

        assertThat(metricsByCommand.get(StatementType.INSERT).totalCount()).isEqualTo(1L);
        assertThat(metricsByCommand.get(StatementType.INSERT).failedCount()).isEqualTo(0L);
        assertThat(metricsByCommand.get(StatementType.INSERT).sumOfDurations()).isEqualTo(19L);

        assertThat(metricsByCommand.get(StatementType.UPDATE).totalCount()).isEqualTo(1L);
        assertThat(metricsByCommand.get(StatementType.UPDATE).failedCount()).isEqualTo(0L);
        assertThat(metricsByCommand.get(StatementType.UPDATE).sumOfDurations()).isEqualTo(20L);

        assertThat(metricsByCommand.get(StatementType.DELETE).totalCount()).isEqualTo(2L);
        assertThat(metricsByCommand.get(StatementType.DELETE).failedCount()).isEqualTo(0L);
        assertThat(metricsByCommand.get(StatementType.DELETE).sumOfDurations()).isEqualTo(15L);

        assertThat(metricsByCommand.get(StatementType.DDL).totalCount()).isEqualTo(1L);
        assertThat(metricsByCommand.get(StatementType.DDL).failedCount()).isEqualTo(0L);
        assertThat(metricsByCommand.get(StatementType.DDL).sumOfDurations()).isEqualTo(1L);

        assertThat(metricsByCommand.get(StatementType.COPY).totalCount()).isEqualTo(1L);
        assertThat(metricsByCommand.get(StatementType.COPY).failedCount()).isEqualTo(1L);
        assertThat(metricsByCommand.get(StatementType.COPY).sumOfDurations()).isEqualTo(0L);
    }

    @Test
    public void testSameTypeWithDifferentLabelsClassificationsAreMerged() {
        List<MetricsView> selectMetrics = List.of(
            createMetric(SELECT_CLASSIFICATION, 35),
            createMetric(new Classification(StatementType.SELECT, Set.of("lookup")), 55)
        );
        Map<StatementType, QueryStats.Metric> metricsByCommand = createMetricsMap(selectMetrics);
        assertThat(metricsByCommand).hasSize(1);

        assertThat(metricsByCommand.get(StatementType.SELECT).totalCount()).isEqualTo(2L);
        assertThat(metricsByCommand.get(StatementType.SELECT).sumOfDurations()).isEqualTo(90L);
    }
}
