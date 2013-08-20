package crate.elasticsearch.rest.action.admin.restore;

import crate.elasticsearch.action.dump.DumpAction;
import crate.elasticsearch.action.restore.RestoreAction;
import crate.elasticsearch.rest.action.admin.export.RestExportAction;
import crate.elasticsearch.rest.action.admin.import_.RestImportAction;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest handler for _restore endpoint
 */
public class RestRestoreAction extends RestImportAction {

    @Inject
    public RestRestoreAction(Settings settings, Client client, RestController controller) {
        super(settings, client, controller);
    }

    @Override
    protected Action action() {
        return RestoreAction.INSTANCE;
    }

    @Override
    protected void registerHandlers(RestController controller) {
        controller.registerHandler(POST, "/_restore", this);
        controller.registerHandler(POST, "/{index}/_restore", this);
        controller.registerHandler(POST, "/{index}/{type}/_restore", this);
    }

}
