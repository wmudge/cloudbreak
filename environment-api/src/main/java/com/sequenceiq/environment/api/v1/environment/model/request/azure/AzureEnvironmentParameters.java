package com.sequenceiq.environment.api.v1.environment.model.request.azure;

import javax.validation.Valid;

import com.sequenceiq.environment.api.doc.environment.EnvironmentModelDescription;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiModel;

@ApiModel(value = "AzureEnvironmentV1Parameters")
public class AzureEnvironmentParameters {

    @Valid
    @ApiModelProperty(EnvironmentModelDescription.RESOURCE_GROUP_PARAMETERS)
    private AzureResourceGroup resourceGroup;

    public AzureResourceGroup getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(AzureResourceGroup resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    private AzureEnvironmentParameters(AzureEnvironmentParameters.Builder builder) {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private AzureResourceGroup azureResourceGroup;

        public Builder withAzureResourceGroup(AzureResourceGroup azureResourceGroup) {
            this.azureResourceGroup = azureResourceGroup;
            return this;
        }

        public AzureEnvironmentParameters build() {
            return new AzureEnvironmentParameters(this);
        }
    }
}
