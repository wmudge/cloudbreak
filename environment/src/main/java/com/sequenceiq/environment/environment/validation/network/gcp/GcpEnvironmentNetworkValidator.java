package com.sequenceiq.environment.environment.validation.network.gcp;

import static com.sequenceiq.cloudbreak.common.mappable.CloudPlatform.GCP;

import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.cloudbreak.validation.ValidationResult;
import com.sequenceiq.environment.environment.dto.EnvironmentDto;
import com.sequenceiq.environment.environment.validation.network.EnvironmentNetworkValidator;
import com.sequenceiq.environment.network.dto.NetworkDto;

@Component
public class GcpEnvironmentNetworkValidator implements EnvironmentNetworkValidator {

    @Override
    public void validateDuringFlow(EnvironmentDto environmentDto, NetworkDto networkDto, ValidationResult.ValidationResultBuilder resultBuilder) {

    }

    @Override
    public void validateDuringRequest(NetworkDto networkDto, ValidationResult.ValidationResultBuilder resultBuilder) {

    }

    @Override
    public CloudPlatform getCloudPlatform() {
        return GCP;
    }
}
