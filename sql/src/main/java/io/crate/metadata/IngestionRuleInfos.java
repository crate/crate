package io.crate.metadata;


import io.crate.metadata.rule.ingest.IngestRulesMetaData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyIterator;

public class IngestionRuleInfos implements Iterable<IngestionRuleInfo> {

    private List<IngestionRuleInfo> ingestionRuleInfoList = null;

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
        }
    }

    @Override
    public Iterator<IngestionRuleInfo> iterator() {
        if (ingestionRuleInfoList == null) {
            return emptyIterator();
        }

        return ingestionRuleInfoList.iterator();
    }
}
