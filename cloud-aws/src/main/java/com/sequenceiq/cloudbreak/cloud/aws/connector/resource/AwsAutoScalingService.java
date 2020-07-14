package com.sequenceiq.cloudbreak.cloud.aws.connector.resource;

import static com.sequenceiq.cloudbreak.cloud.aws.connector.resource.AwsResourceConstants.SUSPENDED_PROCESSES;
import static com.sequenceiq.cloudbreak.cloud.aws.scheduler.BackoffCancellablePollingStrategy.getBackoffCancellablePollingStrategy;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.ResumeProcessesRequest;
import com.amazonaws.services.autoscaling.model.SuspendProcessesRequest;
import com.amazonaws.services.autoscaling.model.TerminateInstanceInAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterParameters;
import com.google.common.collect.Lists;
import com.sequenceiq.cloudbreak.cloud.aws.AwsClient;
import com.sequenceiq.cloudbreak.cloud.aws.CloudFormationStackUtil;
import com.sequenceiq.cloudbreak.cloud.aws.client.AmazonAutoScalingRetryClient;
import com.sequenceiq.cloudbreak.cloud.aws.client.AmazonCloudFormationRetryClient;
import com.sequenceiq.cloudbreak.cloud.aws.scheduler.CustomAmazonWaiterProvider;
import com.sequenceiq.cloudbreak.cloud.aws.scheduler.SlowStartCancellablePollingStrategy;
import com.sequenceiq.cloudbreak.cloud.aws.scheduler.StackCancellationCheck;
import com.sequenceiq.cloudbreak.cloud.aws.view.AwsCredentialView;
import com.sequenceiq.cloudbreak.cloud.context.AuthenticatedContext;
import com.sequenceiq.cloudbreak.cloud.model.CloudStack;
import com.sequenceiq.cloudbreak.cloud.model.Group;

@Service
public class AwsAutoScalingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AwsAutoScalingService.class);

    private static final int MAX_INSTANCE_ID_SIZE = 100;

    private static final int EXPECTED_SCALEUP_TIME_MIN_SECONDS = 40;

    @Inject
    private CloudFormationStackUtil cfStackUtil;

    @Inject
    private AwsClient awsClient;

    @Inject
    private CustomAmazonWaiterProvider customAmazonWaiterProvider;

    @Inject
    private CloudFormationStackUtil cloudFormationStackUtil;

    public void suspendAutoScaling(AuthenticatedContext ac, CloudStack stack) {
        AmazonAutoScalingRetryClient amazonASClient = awsClient.createAutoScalingRetryClient(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        for (Group group : stack.getGroups()) {
            String asGroupName = cfStackUtil.getAutoscalingGroupName(ac, group.getName(), ac.getCloudContext().getLocation().getRegion().value());
            LOGGER.info("Suspend autoscaling group '{}'", asGroupName);
            amazonASClient.suspendProcesses(new SuspendProcessesRequest().withAutoScalingGroupName(asGroupName).withScalingProcesses(SUSPENDED_PROCESSES));
        }
    }

    public void resumeAutoScaling(AmazonAutoScalingRetryClient amazonASClient, Collection<String> groupNames, List<String> autoScalingPolicies) {
        for (String groupName : groupNames) {
            LOGGER.info("Resume autoscaling group '{}'", groupName);
            amazonASClient.resumeProcesses(new ResumeProcessesRequest().withAutoScalingGroupName(groupName).withScalingProcesses(autoScalingPolicies));
        }
    }

    public void scheduleStatusChecks(List<Group> groups, AuthenticatedContext ac, AmazonCloudFormationRetryClient cloudFormationClient, Date timeBeforeASUpdate)
            throws AmazonAutoscalingFailed {
        AmazonEC2Client amClient = awsClient.createAccess(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        AmazonAutoScalingClient asClient = awsClient.createAutoScalingClient(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        AmazonAutoScalingRetryClient asRetryClient = awsClient.createAutoScalingRetryClient(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        for (Group group : groups) {
            String asGroupName = cfStackUtil.getAutoscalingGroupName(ac, cloudFormationClient, group.getName());
            LOGGER.debug("Polling Auto Scaling group until new instances are ready. [stack: {}, asGroup: {}]", ac.getCloudContext().getId(),
                    asGroupName);
            waitForGroup(amClient, asClient, asRetryClient, asGroupName, group.getInstancesSize(), ac.getCloudContext().getId());
        }
    }

    public void scheduleStatusChecks(Map<String, Integer> groupsWithSize, AuthenticatedContext ac, Date timeBeforeASUpdate)
            throws AmazonAutoscalingFailed {

        AmazonEC2Client amClient = awsClient.createAccess(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        AmazonAutoScalingClient asClient = awsClient.createAutoScalingClient(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        AmazonAutoScalingRetryClient asRetryClient = awsClient.createAutoScalingRetryClient(new AwsCredentialView(ac.getCloudCredential()),
                ac.getCloudContext().getLocation().getRegion().value());
        for (Map.Entry<String, Integer> groupWithSize : groupsWithSize.entrySet()) {
            String autoScalingGroupName = groupWithSize.getKey();
            Integer expectedSize = groupWithSize.getValue();
            waitForGroup(amClient, asClient, asRetryClient, autoScalingGroupName, expectedSize, ac.getCloudContext().getId());
        }
    }

    private void waitForGroup(AmazonEC2Client amClient, AmazonAutoScalingClient asClient, AmazonAutoScalingRetryClient asRetryClient,
        String autoScalingGroupName, Integer requiredInstanceCount, Long stackId) throws AmazonAutoscalingFailed {
        Waiter<DescribeAutoScalingGroupsRequest> groupInServiceWaiter = asClient.waiters().groupInService();
        PollingStrategy backoff = getBackoffCancellablePollingStrategy(new StackCancellationCheck(stackId));
        PollingStrategy slowStart = SlowStartCancellablePollingStrategy.getExpectedRuntimeCancellablePollingStrategy(new StackCancellationCheck(stackId), EXPECTED_SCALEUP_TIME_MIN_SECONDS);
        try {
            groupInServiceWaiter.run(new WaiterParameters<>(new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(autoScalingGroupName))
                    .withPollingStrategy(backoff));
        } catch (Exception e) {
            throw new AmazonAutoscalingFailed(e.getMessage(), e);
        }

        long startTime = System.currentTimeMillis();
        Waiter<DescribeAutoScalingGroupsRequest> instancesInServiceWaiter = customAmazonWaiterProvider
                .getAutoscalingInstancesInServiceWaiter(asClient, requiredInstanceCount);
        try {
            instancesInServiceWaiter.run(new WaiterParameters<>(new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(autoScalingGroupName))
                    .withPollingStrategy(slowStart));
        } catch (Exception e) {
            throw new AmazonAutoscalingFailed(e.getMessage(), e);
        }
        LOGGER.info("Done with instancesInServiceWaiter. Duration={}", (System.currentTimeMillis() - startTime));

        List<String> instanceIds = cloudFormationStackUtil.getInstanceIds(asRetryClient, autoScalingGroupName);
        if (requiredInstanceCount != 0) {
            List<List<String>> partitionedInstanceIdsList = Lists.partition(instanceIds, MAX_INSTANCE_ID_SIZE);

            Waiter<DescribeInstancesRequest> instanceRunningStateWaiter = amClient.waiters().instanceRunning();
            for (List<String> partitionedInstanceIds : partitionedInstanceIdsList) {
                startTime = System.currentTimeMillis();
                try {
                    instanceRunningStateWaiter.run(new WaiterParameters<>(new DescribeInstancesRequest().withInstanceIds(partitionedInstanceIds))
                            .withPollingStrategy(backoff));
                } catch (Exception e) {
                    throw new AmazonAutoscalingFailed(e.getMessage(), e);
                }
            }
        }
    }

    public void scheduleStatusChecks(List<Group> groups, AuthenticatedContext ac, AmazonCloudFormationRetryClient cloudFormationClient)
            throws AmazonAutoscalingFailed {
        scheduleStatusChecks(groups, ac, cloudFormationClient, null);
    }

    public List<AutoScalingGroup> getAutoscalingGroups(AmazonAutoScalingRetryClient amazonASClient, Set<String> groupNames) {
        return amazonASClient.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(groupNames)).getAutoScalingGroups();
    }

    public void updateAutoscalingGroup(AmazonAutoScalingRetryClient amazonASClient, String groupName, Integer newSize) {
        LOGGER.info("Update '{}' Auto Scaling groups max size to {}, desired capacity to {}", groupName, newSize, newSize);
        amazonASClient.updateAutoScalingGroup(new UpdateAutoScalingGroupRequest()
                .withAutoScalingGroupName(groupName)
                .withMaxSize(newSize)
                .withDesiredCapacity(newSize));
        LOGGER.debug("Updated '{}' Auto Scaling group's desiredCapacity: [to: '{}']", groupName, newSize);
    }

    public void terminateInstance(AmazonAutoScalingRetryClient amazonASClient, String instanceId) {
        amazonASClient.terminateInstance(new TerminateInstanceInAutoScalingGroupRequest().withShouldDecrementDesiredCapacity(true).withInstanceId(instanceId));
    }
}
