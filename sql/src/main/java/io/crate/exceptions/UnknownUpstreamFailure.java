package io.crate.exceptions;

public class UnknownUpstreamFailure extends UnhandledServerException {

    public UnknownUpstreamFailure() {
        super("DownstreamOperation received a failure from the upstream");
    }
}
