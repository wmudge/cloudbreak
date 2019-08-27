package com.sequenceiq.cloudbreak.cloud.openstack.view;

import static org.apache.commons.lang3.StringUtils.deleteWhitespace;

import java.util.Map;

import com.sequenceiq.cloudbreak.cloud.context.AuthenticatedContext;
import com.sequenceiq.cloudbreak.cloud.model.CloudCredential;

public class KeystoneCredentialView {

    public static final String CB_KEYSTONE_V2 = "cb-keystone-v2";

    public static final String CB_KEYSTONE_V3_PROJECT_SCOPE = "cb-keystone-v3-project-scope";

    public static final String CB_KEYSTONE_V3_DOMAIN_SCOPE = "cb-keystone-v3-domain-scope";

    private static final String OPENSTACK = "openstack";

    private static final String CB_KEYPAIR_NAME = "cb";

    private final CloudCredential cloudCredential;

    private final String stackName;

    public KeystoneCredentialView(AuthenticatedContext authenticatedContext) {
        stackName = authenticatedContext.getCloudContext().getName() + '_' + authenticatedContext.getCloudContext().getId();
        cloudCredential = authenticatedContext.getCloudCredential();
    }

    public KeystoneCredentialView(CloudCredential cloudCredential) {
        stackName = "";
        this.cloudCredential = cloudCredential;
    }

    public String getKeyPairName() {
        return String.format("%s-%s-%s-%s", CB_KEYPAIR_NAME, stackName, deleteWhitespace(getName().toLowerCase()), cloudCredential.getId());
    }

    public String getName() {
        return cloudCredential.getName();
    }

    public String getStackName() {
        return stackName;
    }

    public String getUserName() {
        return getOpenstack().get("userName").toString();
    }

    public String getPassword() {
        return getOpenstack().get("password").toString();
    }

    public String getTenantName() {
        return getKeystoneV2().get("tenantName").toString();
    }

    public String getEndpoint() {
        return getOpenstack().get("endpoint").toString();
    }

    public String getUserDomain() {
        return getOpenstack().get("userDomain").toString();
    }

    public String getProjectName() {
        return getOpenstack().get("projectName").toString();
    }

    public String getProjectDomain() {
        return getOpenstack().get("projectDomainName").toString();
    }

    public String getDomainName() {
        return getOpenstack().get("domainName").toString();
    }

    public String getScope() {
        return getOpenstack().get("keystoneAuthScope").toString();
    }

    public String getVersion() {
        return getKeystoneV2() != null && !getKeystoneV2().isEmpty() ? CB_KEYSTONE_V2 : null;
    }

    private Map<String, Object> getKeystoneV2() {
        return (Map<String, Object>) getOpenstack().get("keystoneV2");
    }

    private Map<String, Object> getOpenstack() {
        if (cloudCredential.hasParameter(OPENSTACK)) {
            return (Map<String, Object>) cloudCredential.getParameter(OPENSTACK, Map.class);
        }
        return cloudCredential.getParameters();
    }
}
