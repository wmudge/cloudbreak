package com.sequenceiq.it.cloudbreak.util.azure;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.sequenceiq.it.cloudbreak.util.CloudFunctionality;
import com.sequenceiq.it.cloudbreak.util.azure.azurecloudblob.AzureCloudBlobUtil;
import com.sequenceiq.it.cloudbreak.util.azure.azurevm.action.AzureClientActions;

@Component
public class AzureCloudFunctionality implements CloudFunctionality {

    @Inject
    private AzureClientActions azureClientActions;

    @Inject
    private AzureCloudBlobUtil azureCloudBlobUtil;

    @Override
    public List<String> listInstanceVolumeIds(List<String> instanceIds) {
        return azureClientActions.listInstanceVolumeIds(instanceIds);
    }

    @Override
    public Map<String, Map<String, String>> listTagsByInstanceId(List<String> instanceIds) {
        return azureClientActions.listTagsByInstanceId(instanceIds);
    }

    @Override
    public void deleteInstances(List<String> instanceIds) {
        azureClientActions.deleteInstances(instanceIds);
    }

    @Override
    public void stopInstances(List<String> instanceIds) {
        azureClientActions.stopInstances(instanceIds);
    }

    @Override
    public void cloudStorageInitialize() {
        azureCloudBlobUtil.createContainerIfNotExist();
    }

    @Override
    public void cloudStorageListContainer(String baseLocation) {
        azureCloudBlobUtil.listAllFoldersInAContaier(baseLocation);
    }

    @Override
    public void cloudStorageListContainerFreeIpa(String baseLocation, String clusterName, String crn) {
        azureCloudBlobUtil.listFreeIpaFoldersInAContaier(baseLocation, clusterName, crn);
    }

    @Override
    public void cloudStorageListContainerDataLake(String baseLocation, String clusterName, String crn) {
        azureCloudBlobUtil.listDataLakeFoldersInAContaier(baseLocation, clusterName, crn);
    }

    @Override
    public void cloudStorageDeleteContainer(String baseLocation) {
        azureCloudBlobUtil.cleanupContainer(baseLocation);
    }
}
