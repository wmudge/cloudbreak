package com.sequenceiq.environment.parameters.dto;

public class OpenstackParametersDto {

    private OpenstackParametersDto(Builder builder) {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Builder() {
        }

        public OpenstackParametersDto build() {
            return new OpenstackParametersDto(this);
        }
    }
}
