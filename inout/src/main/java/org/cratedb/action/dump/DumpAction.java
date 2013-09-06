package org.cratedb.action.dump;

import org.cratedb.action.export.ExportRequest;
import org.cratedb.action.export.ExportResponse;
import org.cratedb.client.action.export.ExportRequestBuilder;
import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;


/**
 *
 */
public class DumpAction extends Action<ExportRequest, ExportResponse, ExportRequestBuilder> {

    public static final DumpAction INSTANCE = new DumpAction();
    public static final String NAME = "el-crate-dump";

    private DumpAction() {
        super(NAME);
    }

    @Override
    public ExportResponse newResponse() {
        return new ExportResponse();
    }

    @Override
    public ExportRequestBuilder newRequestBuilder(Client client) {
        return new ExportRequestBuilder(client);
    }

}
