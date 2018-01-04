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

package io.crate.metadata;


import io.crate.metadata.rule.ingest.IngestRulesMetaData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class IngestionRuleInfos implements Iterable<IngestionRuleInfo> {

    private final List<IngestionRuleInfo> ingestionRuleInfoList;

    public IngestionRuleInfos(IngestRulesMetaData ingestRulesMetaData) {
        if (ingestRulesMetaData != null && ingestRulesMetaData.getIngestRules() != null) {
            ingestionRuleInfoList = new ArrayList<>();
            ingestRulesMetaData.getIngestRules().forEach((key, value) -> value.forEach(ingestRule ->
                ingestionRuleInfoList.add(new IngestionRuleInfo(
                    ingestRule.getName(),
                    key,
                    ingestRule.getTargetTable(),
                    ingestRule.getCondition()))
            ));
        } else {
            ingestionRuleInfoList = Collections.emptyList();
        }
    }

    @Override
    public Iterator<IngestionRuleInfo> iterator() {
        return ingestionRuleInfoList.iterator();
    }
}
