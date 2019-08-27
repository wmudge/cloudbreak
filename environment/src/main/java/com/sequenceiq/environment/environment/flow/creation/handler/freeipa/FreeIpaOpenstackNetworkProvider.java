package com.sequenceiq.environment.environment.flow.creation.handler.freeipa;

import java.util.Set;

import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.environment.environment.dto.EnvironmentDto;
import com.sequenceiq.environment.network.dto.OpenstackParams;
import com.sequenceiq.freeipa.api.v1.freeipa.stack.model.common.network.GcpNetworkParameters;
import com.sequenceiq.freeipa.api.v1.freeipa.stack.model.common.network.NetworkRequest;
import com.sequenceiq.freeipa.api.v1.freeipa.stack.model.common.network.OpenStackNetworkParameters;

@Component
public class FreeIpaOpenstackNetworkProvider implements FreeIpaNetworkProvider {

    @Override
    public NetworkRequest provider(EnvironmentDto environment) {
        NetworkRequest networkRequest = new NetworkRequest();
        OpenstackParams openstackParams = environment.getNetwork().getOpenstack();
        OpenStackNetworkParameters openstackNetworkParameters = new OpenStackNetworkParameters();
        openstackNetworkParameters.setNetworkId(openstackParams.getNetworkId());
        openstackNetworkParameters.setNetworkingOption(openstackParams.getNetworkingOption());
        openstackNetworkParameters.setPublicNetId(openstackParams.getPublicNetId());
        openstackNetworkParameters.setRouterId(openstackParams.getRouterId());
        openstackNetworkParameters.setSubnetId(environment.getNetwork().getSubnetIds().iterator().next());
        networkRequest.setOpenstack(openstackNetworkParameters);
        return networkRequest;
    }

    @Override
    public String availabilityZone(NetworkRequest networkRequest, EnvironmentDto environment) {
        GcpNetworkParameters gcpNetworkParameters = networkRequest.getGcp();
        return environment.getNetwork().getSubnetMetas().get(gcpNetworkParameters.getSubnetId()).getAvailabilityZone();
    }

    @Override
    public Set<String> getSubnets(NetworkRequest networkRequest) {
        return Sets.newHashSet(networkRequest.getOpenstack().getSubnetId());
    }

    @Override
    public CloudPlatform cloudPlatform() {
        return CloudPlatform.OPENSTACK;
    }
}
