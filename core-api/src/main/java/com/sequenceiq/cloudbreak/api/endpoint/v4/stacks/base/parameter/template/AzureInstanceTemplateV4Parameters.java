package com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.base.parameter.template;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.base.InstanceTemplateV4ParameterBase;
import com.sequenceiq.cloudbreak.doc.ModelDescriptions.TemplateModelDescription;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class AzureInstanceTemplateV4Parameters extends InstanceTemplateV4ParameterBase {

    @ApiModelProperty(TemplateModelDescription.AZURE_PRIVATE_ID)
    private String privateId;

    @ApiModelProperty(notes = "by default false")
    private Boolean encrypted = Boolean.FALSE;

    @ApiModelProperty(notes = "by default true")
    private Boolean managedDisk = Boolean.TRUE;

    public Boolean getEncrypted() {
        return encrypted;
    }

    public void setEncrypted(Boolean encrypted) {
        this.encrypted = encrypted;
    }

    public Boolean getManagedDisk() {
        return managedDisk;
    }

    public void setManagedDisk(Boolean managedDisk) {
        this.managedDisk = managedDisk;
    }

    public String getPrivateId() {
        return privateId;
    }

    public void setPrivateId(String privateId) {
        this.privateId = privateId;
    }

    @Override
    public void parse(Map<String, Object> parameters) {
        privateId = getParameterOrNull(parameters, "privateId");
        encrypted = getBoolean(parameters, "encrypted");
        managedDisk = getBoolean(parameters, "managedDisk");
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = super.asMap();
        putIfValueNotNull(map, "privateId", privateId);
        putIfValueNotNull(map, "encrypted", encrypted);
        putIfValueNotNull(map, "managedDisk", managedDisk);
        return map;
    }

    @Override
    @JsonIgnore
    @ApiModelProperty(hidden = true)
    public CloudPlatform getCloudPlatform() {
        return CloudPlatform.AZURE;
    }
}
