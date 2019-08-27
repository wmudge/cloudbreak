package com.sequenceiq.environment.parameters.dto;

public class CumulusYarnParametersDto {

    private CumulusYarnParametersDto(Builder builder) {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Builder() {
        }

        public CumulusYarnParametersDto build() {
            return new CumulusYarnParametersDto(this);
        }
    }
}
