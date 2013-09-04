package crate.elasticsearch.blob;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class StartBlobAction extends Action<StartBlobRequest, StartBlobResponse, StartBlobRequestBuilder> {

    public static final StartBlobAction INSTANCE = new StartBlobAction();
    public static final String NAME = "start_blob";

    protected StartBlobAction() {
        super(NAME);
    }

    @Override
    public StartBlobRequestBuilder newRequestBuilder(Client client) {
        return new StartBlobRequestBuilder(client);
    }

    @Override
    public StartBlobResponse newResponse() {
        return new StartBlobResponse();
    }
}
