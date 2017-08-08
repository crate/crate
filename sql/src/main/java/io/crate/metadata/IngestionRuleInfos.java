package io.crate.metadata;


import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import org.elasticsearch.cluster.metadata.MetaData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.emptyIterator;

public class IngestionRuleInfos implements Iterable<IngestionRuleInfo> {

    private final MetaData metaData;

    public IngestionRuleInfos(MetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public Iterator<IngestionRuleInfo> iterator() {
        IngestRulesMetaData ingestRulesMetaData = metaData.custom(IngestRulesMetaData.TYPE);
        if (ingestRulesMetaData == null || ingestRulesMetaData.getIngestRules() == null) {
            return emptyIterator();
        }
        List<IngestionRuleInfo> ingestionRuleInfoList = new ArrayList<>();

        ingestRulesMetaData.getIngestRules().forEach((key, value) -> value.forEach(ingestRule ->
            ingestionRuleInfoList.add(new IngestionRuleInfo(
                ingestRule.getName(),
                key,
                ingestRule.getTargetTable(),
                ingestRule.getCondition()))
        ));

        return ingestionRuleInfoList.iterator();
    }
}
