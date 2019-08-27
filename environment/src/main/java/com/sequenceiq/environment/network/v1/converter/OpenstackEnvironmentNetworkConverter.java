package com.sequenceiq.environment.network.v1.converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.cloud.model.CloudSubnet;
import com.sequenceiq.cloudbreak.cloud.model.network.CreatedCloudNetwork;
import com.sequenceiq.cloudbreak.cloud.model.network.CreatedSubnet;
import com.sequenceiq.cloudbreak.cloud.model.network.SubnetType;
import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.environment.environment.domain.EnvironmentViewConverter;
import com.sequenceiq.environment.network.dao.domain.BaseNetwork;
import com.sequenceiq.environment.network.dao.domain.OpenstackNetwork;
import com.sequenceiq.environment.network.dao.domain.RegistrationType;
import com.sequenceiq.environment.network.dto.NetworkDto;
import com.sequenceiq.environment.network.dto.OpenstackParams;

@Component
public class OpenstackEnvironmentNetworkConverter extends EnvironmentBaseNetworkConverter {

    protected OpenstackEnvironmentNetworkConverter(EnvironmentViewConverter environmentViewConverter) {
        super(environmentViewConverter);
    }

    @Override
    public boolean isApplicableForDwx(CloudSubnet cloudSubnet) {
        return cloudSubnet.getType() == null
                || SubnetType.PUBLIC.equals(cloudSubnet.getType())
                || SubnetType.PRIVATE.equals(cloudSubnet.getType())
                || SubnetType.DWX.equals(cloudSubnet.getType());
    }

    @Override
    public boolean isApplicableForMlx(CloudSubnet cloudSubnet) {
        return cloudSubnet.getType() == null
                || SubnetType.PUBLIC.equals(cloudSubnet.getType())
                || SubnetType.PRIVATE.equals(cloudSubnet.getType())
                || SubnetType.MLX.equals(cloudSubnet.getType());
    }

    @Override
    BaseNetwork createProviderSpecificNetwork(NetworkDto network) {
        OpenstackNetwork openstackNetwork = new OpenstackNetwork();
        OpenstackParams openstackParams = network.getOpenstack();
        if (openstackParams != null) {
            openstackNetwork.setNetworkId(openstackParams.getNetworkId());
            openstackNetwork.setNetworkingOption(openstackParams.getNetworkingOption());
            openstackNetwork.setPublicNetId(openstackParams.getPublicNetId());
            openstackNetwork.setRouterId(openstackParams.getRouterId());
        }
        return openstackNetwork;
    }

    @Override
    public BaseNetwork setCreatedCloudNetwork(BaseNetwork baseNetwork, CreatedCloudNetwork createdCloudNetwork) {
        OpenstackNetwork openstackNetwork = (OpenstackNetwork) baseNetwork;
        openstackNetwork.setNetworkId(createdCloudNetwork.getNetworkId());
        Map<String, Object> properties = createdCloudNetwork.getProperties();
        if (properties == null) {
            properties = new HashMap<>();
        }

        Object publicNetId = properties.get("publicNetId");
        openstackNetwork.setPublicNetId(publicNetId == null ? null : publicNetId.toString());

        Object networkingOption = properties.get("networkingOption");
        openstackNetwork.setNetworkingOption(networkingOption == null ? null : networkingOption.toString());

        Object routerId = properties.get("routerId");
        openstackNetwork.setRouterId(routerId == null ? null : routerId.toString());

        openstackNetwork.setSubnetMetas(createdCloudNetwork.getSubnets().stream()
                .collect(Collectors.toMap(
                        CreatedSubnet::getSubnetId, subnet -> new CloudSubnet(
                                subnet.getSubnetId(),
                                subnet.getSubnetId(),
                                subnet.getAvailabilityZone(),
                                subnet.getCidr(),
                                !subnet.isPublicSubnet(),
                                subnet.isMapPublicIpOnLaunch(),
                                subnet.isIgwAvailable(),
                                SubnetType.PUBLIC)
                        )
                )
        );
        return openstackNetwork;
    }

    @Override
    NetworkDto setProviderSpecificFields(NetworkDto.Builder builder, BaseNetwork network) {
        OpenstackNetwork openstackNetwork = (OpenstackNetwork) network;
        OpenstackParams.OpenstackParamsBuilder openstackParamsBuilder = OpenstackParams.OpenstackParamsBuilder.anOpenstackParams();
        Optional.ofNullable(openstackNetwork.getNetworkId()).ifPresent(openstackParamsBuilder::withNetworkId);
        Optional.ofNullable(openstackNetwork.getNetworkingOption()).ifPresent(openstackParamsBuilder::withNetworkingOption);
        Optional.ofNullable(openstackNetwork.getPublicNetId()).ifPresent(openstackParamsBuilder::withPublicNetId);
        Optional.ofNullable(openstackNetwork.getRouterId()).ifPresent(openstackParamsBuilder::withRouterId);
        return builder.withOpenstack(openstackParamsBuilder.build()).build();
    }

    @Override
    void setRegistrationType(BaseNetwork result, NetworkDto networkDto) {
        if (isExistingNetworkSpecified(networkDto)) {
            result.setRegistrationType(RegistrationType.EXISTING);
        } else {
            result.setRegistrationType(RegistrationType.CREATE_NEW);
        }
    }

    private boolean isExistingNetworkSpecified(NetworkDto networkDto) {
        return networkDto.getOpenstack() != null && networkDto.getOpenstack().getNetworkId() != null;
    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return CloudPlatform.OPENSTACK;
    }

}
