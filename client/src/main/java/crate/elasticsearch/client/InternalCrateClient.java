package crate.elasticsearch.client;

import com.google.common.collect.ImmutableMap;
import crate.elasticsearch.action.sql.SQLAction;
import crate.elasticsearch.action.sql.SQLRequest;
import crate.elasticsearch.action.sql.SQLResponse;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.TransportService;

public class InternalCrateClient {

    private final ImmutableMap<Action, TransportActionNodeProxy> actions;
    private final TransportClientNodesService nodesService;

    @Inject
    public InternalCrateClient(Settings settings,
            TransportService transportService,
            TransportClientNodesService nodesService
                              ) {

        this.nodesService = nodesService;

        // Currently we only support the sql action, so this gets registered directly
        MapBuilder<Action, TransportActionNodeProxy> actionsBuilder = new MapBuilder<Action,
                TransportActionNodeProxy>();
        actionsBuilder.put((Action) SQLAction.INSTANCE,
                new TransportActionNodeProxy(settings, SQLAction.INSTANCE, transportService));
        this.actions = actionsBuilder.immutableMap();
    }

    public ActionFuture<SQLResponse> sql(final SQLRequest request) {
        return execute(SQLAction.INSTANCE, request);
    }


    protected <Request extends ActionRequest, Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response,
                    RequestBuilder>> ActionFuture<Response> execute(final Action<Request,
            Response, RequestBuilder> action, final Request request) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        return nodesService.execute(
                new TransportClientNodesService.NodeCallback<ActionFuture<Response>>() {
                    @Override
                    public ActionFuture<Response> doWithNode(DiscoveryNode node) throws
                            ElasticSearchException {
                        return proxy.execute(node, request);
                    }
                });
    }

    public void addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
    }
}
