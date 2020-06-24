package com.sequenceiq.cloudbreak.cloud.model.instance;

import java.util.Collection;
import java.util.Map;

import com.sequenceiq.cloudbreak.cloud.model.InstanceStatus;
import com.sequenceiq.cloudbreak.cloud.model.InstanceTemplate;
import com.sequenceiq.cloudbreak.cloud.model.Volume;

public class AwsInstanceTemplate extends InstanceTemplate {

    /**
     * Key of the optional dynamic parameter denoting whether EBS encryption is enabled or not. This applies to both root & attached (data) volumes.
     *
     * <p>
     *     Permitted values:
     *     <ul>
     *         <li>{@code Boolean.TRUE} instance, {@code "true"} (ignoring case): Encryption is enabled.</li>
     *         <li>{@code Boolean.FALSE} instance, {@code "false"} (or any other {@code String} not equal to {@code "true"} ignoring case), {@code null}:
     *         Encryption is disabled.</li>
     *     </ul>
     * </p>
     *
     * @see #putParameter(String, Object)
     * @see Boolean#parseBoolean(String)
     */
    public static final String EBS_ENCRYPTION_ENABLED = "encrypted";

    /**
     * Key of the optional dynamic parameter denoting the percentage of EC2 instances to be allocated to spot instances. The remaining fraction of the total
     * amount of EC2 instances will be running on on-demand instances.
     *
     * <p>
     *     Permitted values:
     *     <ul>
     *         <li>{@code null}: Equivalent with a setting of 0 (i.e. 100% allocation to on-demand instances).</li>
     *         <li>An {@link Integer} {@code i} in the range [0, 100], both inclusive: {@code i}% allocation to spot instances, (100 - {@code i})% allocation
     *         to on-demand instances.</li>
     *     </ul>
     * </p>
     *
     * @see #putParameter(String, Object)
     */
    public static final String EC2_SPOT_PERCENTAGE = "spotPercentage";

    /**
     * Key of the optional dynamic parameter denoting whether speed optimizations for the EBS encryption setup logic are enabled or not. This applies to both
     * root & attached (data) volumes.
     *
     * <p>
     *     Permitted values:
     *     <ul>
     *         <li>{@code Boolean.TRUE} instance, {@code "true"} (ignoring case): EBS encryption will be set up using the new fast logic.</li>
     *         <li>{@code Boolean.FALSE} instance, {@code "false"} (or any other {@code String} not equal to {@code "true"} ignoring case), {@code null}:
     *         EBS encryption provisioning will utilize the legacy slow logic.</li>
     *     </ul>
     * </p>
     *
     * @see #putParameter(String, Object)
     * @see Boolean#parseBoolean(String)
     */
    public static final String FAST_EBS_ENCRYPTION_ENABLED = "fastEbsEncryption";

    public AwsInstanceTemplate(String flavor, String groupName, Long privateId, Collection<Volume> volumes, InstanceStatus status,
            Map<String, Object> parameters, Long templateId, String imageId) {
        super(flavor, groupName, privateId, volumes, status, parameters, templateId, imageId);
    }

}
