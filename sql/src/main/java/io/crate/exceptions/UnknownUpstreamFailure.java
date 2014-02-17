package io.crate.exceptions;

import org.cratedb.sql.CrateException;

public class UnknownUpstreamFailure extends CrateException {

    public UnknownUpstreamFailure() {
        super("DownstreamOperation received a failure from the upstream");
    }
}
