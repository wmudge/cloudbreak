package com.sequenceiq.environment.network.dto;

public class OpenstackParams {

    private String networkId;

    private String routerId;

    private String publicNetId;

    private String networkingOption;

    public String getNetworkId() {
        return networkId;
    }

    public void setNetworkId(String networkId) {
        this.networkId = networkId;
    }

    public String getRouterId() {
        return routerId;
    }

    public void setRouterId(String routerId) {
        this.routerId = routerId;
    }

    public String getPublicNetId() {
        return publicNetId;
    }

    public void setPublicNetId(String publicNetId) {
        this.publicNetId = publicNetId;
    }

    public String getNetworkingOption() {
        return networkingOption;
    }

    public void setNetworkingOption(String networkingOption) {
        this.networkingOption = networkingOption;
    }

    public static final class OpenstackParamsBuilder {
        private String networkId;

        private String routerId;

        private String publicNetId;

        private String networkingOption;

        private OpenstackParamsBuilder() {
        }

        public static OpenstackParamsBuilder anOpenstackParams() {
            return new OpenstackParamsBuilder();
        }

        public OpenstackParamsBuilder withNetworkId(String networkId) {
            this.networkId = networkId;
            return this;
        }

        public OpenstackParamsBuilder withRouterId(String routerId) {
            this.routerId = routerId;
            return this;
        }

        public OpenstackParamsBuilder withPublicNetId(String publicNetId) {
            this.publicNetId = publicNetId;
            return this;
        }

        public OpenstackParamsBuilder withNetworkingOption(String networkingOption) {
            this.networkingOption = networkingOption;
            return this;
        }

        public OpenstackParams build() {
            OpenstackParams openstackParams = new OpenstackParams();
            openstackParams.setNetworkId(networkId);
            openstackParams.setNetworkingOption(networkingOption);
            openstackParams.setPublicNetId(publicNetId);
            openstackParams.setRouterId(routerId);
            return openstackParams;
        }
    }
}
