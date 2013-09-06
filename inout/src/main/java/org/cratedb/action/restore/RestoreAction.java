package org.cratedb.action.restore;

import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.import_.ImportResponse;
import org.cratedb.client.action.import_.ImportRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;


/**
 *
 */
public class RestoreAction extends Action<ImportRequest, ImportResponse, ImportRequestBuilder> {

    public static final RestoreAction INSTANCE = new RestoreAction();
    public static final String NAME = "el-crate-restore";

    private RestoreAction() {
        super(NAME);
    }

    @Override
    public ImportResponse newResponse() {
        return new ImportResponse();
    }

    @Override
    public ImportRequestBuilder newRequestBuilder(Client client) {
        return new ImportRequestBuilder(client);
    }

}
