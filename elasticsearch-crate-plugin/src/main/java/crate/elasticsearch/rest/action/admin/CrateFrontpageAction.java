package crate.elasticsearch.rest.action.admin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestHandlerAction to return a html page containing informations about crate
 */
public class CrateFrontpageAction extends BaseRestHandler {

    @Inject
    public CrateFrontpageAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/admin", this);
    }

    @Override
    public void handleRequest(RestRequest restRequest, RestChannel restChannel) {
        StringRestResponse resp = new StringRestResponse(RestStatus.TEMPORARY_REDIRECT);
        resp.addHeader("Location", "/_plugin/crate-admin/");
        restChannel.sendResponse(resp);
    }

}
