package org.cratedb.action.sql.analyzer;

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.ClusterAdminClient;

public class ClusterUpdateCrateSettingsAction extends ClusterAction<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse, ClusterUpdateSettingsRequestBuilder> {

    public static final ClusterUpdateCrateSettingsAction INSTANCE = new ClusterUpdateCrateSettingsAction();
    public static final String NAME = "crate/cluster/settings/update";

    private ClusterUpdateCrateSettingsAction() {
        super(NAME);
    }

    @Override
    public ClusterUpdateSettingsResponse newResponse() {
        return new ClusterUpdateSettingsResponse();
    }

    @Override
    public ClusterUpdateSettingsRequestBuilder newRequestBuilder(ClusterAdminClient client) {
        return new ClusterUpdateSettingsRequestBuilder(client);
    }
}