package com.sequenceiq.datalake.service.sdx;

import static com.sequenceiq.cloudbreak.util.NullUtil.getIfNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sequenceiq.cloudbreak.api.endpoint.v4.common.StackType;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.base.parameter.network.AwsNetworkV4Parameters;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.base.parameter.network.AzureNetworkV4Parameters;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.base.parameter.stack.YarnStackV4Parameters;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.StackV4Request;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.authentication.StackAuthenticationV4Request;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.cluster.ClusterV4Request;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.environment.placement.PlacementSettingsV4Request;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.instancegroup.InstanceGroupV4Request;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.network.NetworkV4Request;
import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.request.tags.TagsV4Request;
import com.sequenceiq.cloudbreak.auth.altus.Crn;
import com.sequenceiq.cloudbreak.auth.security.InternalCrnBuilder;
import com.sequenceiq.cloudbreak.cloud.model.CloudSubnet;
import com.sequenceiq.cloudbreak.common.json.JsonUtil;
import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.cloudbreak.idbmms.GrpcIdbmmsClient;
import com.sequenceiq.cloudbreak.idbmms.exception.IdbmmsOperationException;
import com.sequenceiq.cloudbreak.idbmms.model.MappingsConfig;
import com.sequenceiq.cloudbreak.util.PasswordUtil;
import com.sequenceiq.common.api.cloudstorage.AccountMappingBase;
import com.sequenceiq.common.api.cloudstorage.CloudStorageRequest;
import com.sequenceiq.common.api.telemetry.request.FeaturesRequest;
import com.sequenceiq.common.api.telemetry.request.LoggingRequest;
import com.sequenceiq.common.api.telemetry.request.TelemetryRequest;
import com.sequenceiq.common.api.type.InstanceGroupType;
import com.sequenceiq.datalake.controller.exception.BadRequestException;
import com.sequenceiq.datalake.entity.SdxCluster;
import com.sequenceiq.datalake.service.validation.cloudstorage.CloudStorageValidator;
import com.sequenceiq.environment.api.v1.environment.model.base.IdBrokerMappingSource;
import com.sequenceiq.environment.api.v1.environment.model.response.DetailedEnvironmentResponse;
import com.sequenceiq.environment.api.v1.environment.model.response.EnvironmentNetworkResponse;
import com.sequenceiq.environment.api.v1.environment.model.response.SecurityAccessResponse;
import com.sequenceiq.sdx.api.model.SdxClusterRequest;

@Service
public class StackRequestManifester {

    static final String IAM_INTERNAL_ACTOR_CRN = new InternalCrnBuilder(Crn.Service.IAM).getInternalCrnForServiceAsString();

    private static final Logger LOGGER = LoggerFactory.getLogger(StackRequestManifester.class);

    @Inject
    private GatewayManifester gatewayManifester;

    @Inject
    private CloudStorageManifester cloudStorageManifester;

    @Inject
    private CloudStorageValidator cloudStorageValidator;

    @Inject
    private GrpcIdbmmsClient idbmmsClient;

    @Inject
    private SecurityAccessManifester securityAccessManifester;

    public void configureStackForSdxCluster(SdxClusterRequest sdxClusterRequest, SdxCluster sdxCluster,
            StackV4Request stackRequest, DetailedEnvironmentResponse environment) {
        StackV4Request generatedStackV4Request = setupStackRequestForCloudbreak(sdxClusterRequest, sdxCluster, stackRequest, environment);
        gatewayManifester.configureGatewayForSdxCluster(generatedStackV4Request);
        addStackV4RequestAsString(sdxCluster, generatedStackV4Request);
    }

    private void addStackV4RequestAsString(SdxCluster sdxCluster, StackV4Request internalRequest) {
        try {
            LOGGER.info("Forming request from Internal Request");
            sdxCluster.setStackRequestToCloudbreak(JsonUtil.writeValueAsString(internalRequest));
        } catch (JsonProcessingException e) {
            LOGGER.error("Can not serialize stack request as JSON");
            throw new BadRequestException("Can not serialize stack request as JSON", e);
        }
    }

    private StackV4Request setupStackRequestForCloudbreak(SdxClusterRequest sdxClusterRequest,
            SdxCluster sdxCluster, StackV4Request stackRequest, DetailedEnvironmentResponse environment) {
        LOGGER.info("Setting up stack request of SDX {} for cloudbreak", sdxCluster.getClusterName());
        stackRequest.setName(sdxCluster.getClusterName());
        stackRequest.setType(StackType.DATALAKE);
        if (stackRequest.getTags() == null) {
            TagsV4Request tags = new TagsV4Request();
            try {
                tags.setUserDefined(sdxCluster.getTags().get(HashMap.class));
            } catch (IOException e) {
                LOGGER.error("Can not parse JSON to tags");
                throw new BadRequestException("Can not parse JSON to tags", e);
            }
            stackRequest.setTags(tags);
        }
        stackRequest.setEnvironmentCrn(sdxCluster.getEnvCrn());

        if (CloudPlatform.YARN.name().equals(environment.getCloudPlatform())) {
            setupYarnDetails(environment, stackRequest);
        }

        if (environment.getNetwork() != null
                && environment.getNetwork().getSubnetMetas() != null
                && !environment.getNetwork().getSubnetMetas().isEmpty()) {
            CloudSubnet cloudSubnet = getSubnet(environment.getNetwork());
            setupPlacement(environment, cloudSubnet, stackRequest);
            setupNetwork(environment, cloudSubnet, stackRequest);
        }

        setupAuthentication(environment, stackRequest);
        setupSecurityAccess(environment, stackRequest);
        setupClusterRequest(stackRequest);
        prepareTelemetryForStack(stackRequest, environment);
        prepareCloudStorageForStack(sdxClusterRequest, stackRequest, sdxCluster, environment);
        setupCloudStorageAccountMapping(stackRequest, environment.getCrn(), environment.getIdBrokerMappingSource(), environment.getCloudPlatform());
        cloudStorageValidator.validate(stackRequest.getCluster().getCloudStorage(), environment);
        return stackRequest;
    }

    private CloudSubnet getSubnet(EnvironmentNetworkResponse network) {
        return network.isExistingNetwork()
                ? network.getSubnetMetas().values().stream().findFirst().orElseThrow(getException())
                : network.getSubnetMetas().entrySet().stream()
                .filter(entry -> !entry.getValue().isPrivateSubnet()).findFirst()
                .map(Map.Entry::getValue)
                .orElseThrow(getException());
    }

    private Supplier<BadRequestException> getException() {
        return () -> new BadRequestException("No subnet id for this environment");
    }

    private void setupYarnDetails(DetailedEnvironmentResponse environment, StackV4Request stackRequest) {
        if (stackRequest.getYarn() == null || stackRequest.getYarn().getYarnQueue() == null) {
            if (environment.getNetwork() == null
                    || environment.getNetwork().getYarn() == null
                    || environment.getNetwork().getYarn().getQueue() == null) {
                throw new BadRequestException("There is no queue defined in your environment, please create a new yarn environment with queue");
            } else {
                YarnStackV4Parameters yarnStackV4Parameters = new YarnStackV4Parameters();
                yarnStackV4Parameters.setYarnQueue(environment.getNetwork().getYarn().getQueue());
                stackRequest.setYarn(yarnStackV4Parameters);
            }
        }
    }

    private void setupPlacement(DetailedEnvironmentResponse environment, CloudSubnet cloudSubnet, StackV4Request stackRequest) {
        PlacementSettingsV4Request placementSettingsV4Request = new PlacementSettingsV4Request();
        placementSettingsV4Request.setAvailabilityZone(cloudSubnet.getAvailabilityZone());
        placementSettingsV4Request.setRegion(environment.getRegions().getNames().iterator().next());
        stackRequest.setPlacement(placementSettingsV4Request);
    }

    private void setupNetwork(DetailedEnvironmentResponse environmentResponse, CloudSubnet cloudSubnet, StackV4Request stackRequest) {
        stackRequest.setNetwork(convertNetwork(environmentResponse.getNetwork(), cloudSubnet));
    }

    private NetworkV4Request convertNetwork(EnvironmentNetworkResponse network, CloudSubnet cloudSubnet) {
        NetworkV4Request response = new NetworkV4Request();
        response.setAws(getIfNotNull(network.getAws(), aws -> convertToAwsNetwork(network, cloudSubnet)));
        response.setAzure(getIfNotNull(network.getAzure(), azure -> convertToAzureNetwork(network, cloudSubnet)));
        return response;
    }

    private AzureNetworkV4Parameters convertToAzureNetwork(EnvironmentNetworkResponse source, CloudSubnet cloudSubnet) {
        AzureNetworkV4Parameters response = new AzureNetworkV4Parameters();
        response.setNetworkId(source.getAzure().getNetworkId());
        response.setNoFirewallRules(source.getAzure().getNoFirewallRules());
        response.setNoPublicIp(source.getAzure().getNoPublicIp());
        response.setResourceGroupName(source.getAzure().getResourceGroupName());
        response.setSubnetId(cloudSubnet.getId());
        return response;
    }

    private AwsNetworkV4Parameters convertToAwsNetwork(EnvironmentNetworkResponse source, CloudSubnet cloudSubnet) {
        AwsNetworkV4Parameters response = new AwsNetworkV4Parameters();
        response.setSubnetId(cloudSubnet.getId());
        response.setVpcId(source.getAws().getVpcId());
        return response;
    }

    private void setupAuthentication(DetailedEnvironmentResponse environment, StackV4Request stackRequest) {
        if (stackRequest.getAuthentication() == null) {
            StackAuthenticationV4Request stackAuthenticationV4Request = new StackAuthenticationV4Request();
            stackAuthenticationV4Request.setPublicKey(environment.getAuthentication().getPublicKey());
            stackAuthenticationV4Request.setPublicKeyId(environment.getAuthentication().getPublicKeyId());
            stackRequest.setAuthentication(stackAuthenticationV4Request);
        }
    }

    private void setupSecurityAccess(DetailedEnvironmentResponse environment, StackV4Request stackRequest) {
        List<InstanceGroupV4Request> instanceGroups = stackRequest.getInstanceGroups();
        SecurityAccessResponse securityAccess = environment.getSecurityAccess();
        if (instanceGroups != null && securityAccess != null) {
            String securityGroupIdForKnox = securityAccess.getSecurityGroupIdForKnox();
            String defaultSecurityGroupId = securityAccess.getDefaultSecurityGroupId();
            String cidrs = securityAccess.getCidr();
            securityAccessManifester.overrideSecurityAccess(InstanceGroupType.GATEWAY, instanceGroups, securityGroupIdForKnox, cidrs);
            securityAccessManifester.overrideSecurityAccess(InstanceGroupType.CORE, instanceGroups, defaultSecurityGroupId, cidrs);
        }
    }

    private void setupClusterRequest(StackV4Request stackRequest) {
        ClusterV4Request cluster = stackRequest.getCluster();
        if (cluster != null && cluster.getBlueprintName() == null) {
            throw new BadRequestException("BlueprintName not defined, should only happen on private API");
        }
        if (cluster != null && cluster.getUserName() == null) {
            cluster.setUserName("admin");
        }
        if (cluster != null && cluster.getPassword() == null) {
            cluster.setPassword(PasswordUtil.generatePassword());
        }
    }

    private void prepareTelemetryForStack(StackV4Request stackV4Request, DetailedEnvironmentResponse environment) {
        if (environment.getTelemetry() != null && environment.getTelemetry().getLogging() != null) {
            TelemetryRequest telemetryRequest = new TelemetryRequest();
            LoggingRequest loggingRequest = new LoggingRequest();
            loggingRequest.setS3(environment.getTelemetry().getLogging().getS3());
            loggingRequest.setAdlsGen2(environment.getTelemetry().getLogging().getAdlsGen2());
            loggingRequest.setStorageLocation(environment.getTelemetry().getLogging().getStorageLocation());
            telemetryRequest.setLogging(loggingRequest);
            if (environment.getTelemetry().getFeatures() != null
                    && environment.getTelemetry().getFeatures().getReportDeploymentLogs() != null) {
                FeaturesRequest featuresRequest = new FeaturesRequest();
                featuresRequest.setReportDeploymentLogs(
                        environment.getTelemetry().getFeatures().getReportDeploymentLogs());
                telemetryRequest.setFeatures(featuresRequest);
            }
            stackV4Request.setTelemetry(telemetryRequest);
        }
    }

    private void prepareCloudStorageForStack(SdxClusterRequest sdxClusterRequest, StackV4Request stackV4Request,
            SdxCluster sdxCluster, DetailedEnvironmentResponse environment) {
        CloudStorageRequest cloudStorageRequest = cloudStorageManifester.initCloudStorageRequest(environment,
                stackV4Request.getCluster(), sdxCluster, sdxClusterRequest);
        stackV4Request.getCluster().setCloudStorage(cloudStorageRequest);
    }

    void setupCloudStorageAccountMapping(StackV4Request stackRequest, String environmentCrn, IdBrokerMappingSource mappingSource, String cloudPlatform) {
        String stackName = stackRequest.getName();
        CloudStorageRequest cloudStorage = stackRequest.getCluster().getCloudStorage();
        if (cloudStorage != null && cloudStorage.getAccountMapping() == null) {
            // In case of SdxClusterRequest with cloud storage, or SdxInternalClusterRequest with cloud storage but missing "accountMapping" property,
            // getAccountMapping() == null means we need to fetch mappings from IDBMMS.
            if (mappingSource == IdBrokerMappingSource.IDBMMS) {
                LOGGER.info("Fetching account mappings from IDBMMS associated with environment {} for stack {}.", environmentCrn, stackName);
                // Must pass the internal actor here as this operation is internal-use only; requests with other actors will be always rejected.
                MappingsConfig mappingsConfig;
                try {
                    mappingsConfig = idbmmsClient.getMappingsConfig(IAM_INTERNAL_ACTOR_CRN, environmentCrn, Optional.empty());
                } catch (IdbmmsOperationException e) {
                    throw new BadRequestException(String.format("Unable to get mappings: %s",
                            e.getMessage()), e);
                }
                AccountMappingBase accountMapping = new AccountMappingBase();
                accountMapping.setGroupMappings(mappingsConfig.getGroupMappings());
                accountMapping.setUserMappings(mappingsConfig.getActorMappings());
                cloudStorage.setAccountMapping(accountMapping);
            } else {
                LOGGER.info("IDBMMS usage is disabled for environment {}. Proceeding with {} mappings for stack {}.", environmentCrn,
                        mappingSource == IdBrokerMappingSource.MOCK
                                && (CloudPlatform.AWS.name().equals(cloudPlatform)
                                || CloudPlatform.AZURE.name().equals(cloudPlatform)) ? "mock" : "missing", stackName);
            }
        } else {
            // getAccountMapping() != null is possible only in case of SdxInternalClusterRequest, in which case the user-given values will be honored.
            LOGGER.info("{} for stack {} in environment {}.", cloudStorage == null ? "Cloud storage is disabled" : "Applying user-provided mappings",
                    stackName, environmentCrn);
        }
    }
}
