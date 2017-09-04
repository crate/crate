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
