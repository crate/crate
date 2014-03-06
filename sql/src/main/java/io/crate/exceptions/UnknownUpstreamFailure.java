package io.crate.exceptions;

public class UnknownUpstreamFailure extends CrateException {

    public UnknownUpstreamFailure() {
        super("DownstreamOperation received a failure from the upstream");
    }
}
