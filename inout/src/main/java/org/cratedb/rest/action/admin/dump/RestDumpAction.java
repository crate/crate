package org.cratedb.rest.action.admin.dump;

import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import org.cratedb.action.dump.DumpAction;
import org.cratedb.action.export.ExportRequest;
import org.cratedb.action.export.ExportResponse;
import org.cratedb.client.action.export.ExportRequestBuilder;
import org.cratedb.rest.action.admin.export.RestExportAction;

/**
 * Rest handler for _dump endpoint
 */
public class RestDumpAction extends RestExportAction {

    @Inject
    public RestDumpAction(Settings settings, Client client, RestController controller) {
        super(settings, client, controller);
    }

    @Override
    protected Action<ExportRequest, ExportResponse, ExportRequestBuilder> action() {
        return DumpAction.INSTANCE;
    }

    @Override
    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_dump", this);
        controller.registerHandler(POST, "/{index}/_dump", this);
        controller.registerHandler(POST, "/{index}/{type}/_dump", this);
    }

}
