package com.sequenceiq.environment.network.dto;

public class GcpParams {

    private String networkId;

    private String sharedProjectId;

    private Boolean noPublicIp;

    private Boolean noFirewallRules;

    public String getNetworkId() {
        return networkId;
    }

    public void setNetworkId(String networkId) {
        this.networkId = networkId;
    }

    public String getSharedProjectId() {
        return sharedProjectId;
    }

    public void setSharedProjectId(String sharedProjectId) {
        this.sharedProjectId = sharedProjectId;
    }

    public Boolean getNoPublicIp() {
        return noPublicIp;
    }

    public void setNoPublicIp(Boolean noPublicIp) {
        this.noPublicIp = noPublicIp;
    }

    public Boolean getNoFirewallRules() {
        return noFirewallRules;
    }

    public void setNoFirewallRules(Boolean noFirewallRules) {
        this.noFirewallRules = noFirewallRules;
    }

    public static final class GcpParamsBuilder {

        private String networkId;

        private String sharedProjectId;

        private Boolean noPublicIp;

        private Boolean noFirewallRules;

        private GcpParamsBuilder() {
        }

        public static GcpParamsBuilder aGcpParams() {
            return new GcpParamsBuilder();
        }

        public GcpParamsBuilder withNetworkId(String networkId) {
            this.networkId = networkId;
            return this;
        }

        public GcpParamsBuilder withSharedProjectId(String sharedProjectId) {
            this.sharedProjectId = sharedProjectId;
            return this;
        }

        public GcpParamsBuilder withNoPublicIp(Boolean noPublicIp) {
            this.noPublicIp = noPublicIp;
            return this;
        }

        public GcpParamsBuilder withNoFirewallRules(Boolean noFirewallRules) {
            this.noFirewallRules = noFirewallRules;
            return this;
        }

        public GcpParams build() {
            GcpParams gcpParams = new GcpParams();
            gcpParams.setNetworkId(networkId);
            gcpParams.setSharedProjectId(sharedProjectId);
            gcpParams.setNoPublicIp(noPublicIp);
            gcpParams.setNoFirewallRules(noFirewallRules);
            return gcpParams;
        }
    }
}
