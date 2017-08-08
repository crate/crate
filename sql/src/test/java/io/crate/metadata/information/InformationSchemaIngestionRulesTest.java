package io.crate.metadata.information;


import com.google.common.collect.Iterators;
import io.crate.metadata.IngestionRuleInfo;
import io.crate.metadata.IngestionRuleInfos;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.metadata.rule.ingest.IngestRulesMetaData;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.is;


public class InformationSchemaIngestionRulesTest extends CrateDummyClusterServiceUnitTest {

    private IngestionRuleInfos ingestionRuleInfos;

    public void setUpMetaDataForTests(Boolean emptyMetaData) throws Exception {
        if (emptyMetaData == false) {
            Map<String, Set<IngestRule>> sourceRules = new HashMap<>();

            Set<IngestRule> rules_mqtt = new HashSet<>();
            rules_mqtt.add(new IngestRule("processV4TopicRule", "mqtt_raw", "topic like v4/%"));
            rules_mqtt.add(new IngestRule("processV5TopicRule", "mqtt_5_raw", "topic like v5/%"));
            sourceRules.put("mqtt", rules_mqtt);

            Set<IngestRule> rules_http = new HashSet<>();
            rules_http.add(new IngestRule("http_rule", "http_raw", "topic like http/%"));
            sourceRules.put("http", rules_http);

            IngestRulesMetaData inputMetaData = new IngestRulesMetaData(sourceRules);

            ClusterState clusterState = ClusterState.builder(clusterService.state())
                .metaData(MetaData.builder(clusterService.state().metaData())
                    .putCustom(IngestRulesMetaData.TYPE, inputMetaData)).build();
            ClusterServiceUtils.setState(clusterService, clusterState);
        }
        ingestionRuleInfos = new IngestionRuleInfos(clusterService.state().getMetaData());
    }

    @Test
    public void testIngestionRulesInfosReturnCorrectIterator() throws Exception {
        setUpMetaDataForTests(false);
        assertThat(Iterators.size(ingestionRuleInfos.iterator()), is(3));

        IngestionRuleInfo httpRule = StreamSupport.stream(ingestionRuleInfos.spliterator(), false)
            .filter(item -> "http_rule".equals(item.getName()))
            .collect(Collectors.toList()).get(0);

        assertThat(httpRule.getCondition(), is("topic like http/%"));
        assertThat(httpRule.getSource(), is("http"));
        assertThat(httpRule.getTarget(), is("http_raw"));

        IngestionRuleInfo mgttV4Rule = StreamSupport.stream(ingestionRuleInfos.spliterator(), false)
            .filter(item -> "processV4TopicRule".equals(item.getName()))
            .collect(Collectors.toList()).get(0);

        assertThat(mgttV4Rule.getCondition(), is("topic like v4/%"));
        assertThat(mgttV4Rule.getSource(), is("mqtt"));
        assertThat(mgttV4Rule.getTarget(), is("mqtt_raw"));

        IngestionRuleInfo mgttV5Rule = StreamSupport.stream(ingestionRuleInfos.spliterator(), false)
            .filter(item -> "processV5TopicRule".equals(item.getName()))
            .collect(Collectors.toList()).get(0);

        assertThat(mgttV5Rule.getCondition(), is("topic like v5/%"));
        assertThat(mgttV5Rule.getSource(), is("mqtt"));
        assertThat(mgttV5Rule.getTarget(), is("mqtt_5_raw"));

    }

    @Test
    public void testEmptyMetadataReturnsEmptyIterator() throws Exception {
        setUpMetaDataForTests(true);
        assertThat(Iterators.size(ingestionRuleInfos.iterator()), is(0));
    }
}
