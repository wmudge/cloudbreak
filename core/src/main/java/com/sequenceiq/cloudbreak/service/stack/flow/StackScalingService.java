package com.sequenceiq.cloudbreak.service.stack.flow;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.api.model.stack.instance.InstanceStatus;
import com.sequenceiq.cloudbreak.common.type.BillingStatus;
import com.sequenceiq.cloudbreak.domain.stack.Stack;
import com.sequenceiq.cloudbreak.domain.stack.instance.InstanceGroup;
import com.sequenceiq.cloudbreak.domain.stack.instance.InstanceMetaData;
import com.sequenceiq.cloudbreak.repository.InstanceMetaDataRepository;
import com.sequenceiq.cloudbreak.service.TransactionService;
import com.sequenceiq.cloudbreak.service.TransactionService.TransactionExecutionException;
import com.sequenceiq.cloudbreak.service.cluster.ambari.AmbariDecommissioner;
import com.sequenceiq.cloudbreak.service.events.CloudbreakEventService;
import com.sequenceiq.cloudbreak.service.hostgroup.HostGroupService;
import com.sequenceiq.cloudbreak.service.messages.CloudbreakMessagesService;

@Service
public class StackScalingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(StackScalingService.class);

    @Inject
    private CloudbreakEventService eventService;

    @Inject
    private InstanceMetaDataRepository instanceMetaDataRepository;

    @Inject
    private HostGroupService hostGroupService;

    @Inject
    private AmbariDecommissioner ambariDecommissioner;

    @Inject
    private CloudbreakMessagesService cloudbreakMessagesService;

    @Inject
    private TransactionService transactionService;

    public int updateRemovedResourcesState(Stack stack, Collection<String> instanceIds, InstanceGroup instanceGroup) throws TransactionExecutionException {
        return transactionService.required(() -> {
            int nodesRemoved = 0;
            for (InstanceMetaData instanceMetaData : instanceGroup.getNotTerminatedInstanceMetaDataSet()) {
                if (instanceIds.contains(instanceMetaData.getInstanceId())) {
                    long timeInMillis = Calendar.getInstance().getTimeInMillis();
                    instanceMetaData.setTerminationDate(timeInMillis);
                    instanceMetaData.setInstanceStatus(InstanceStatus.TERMINATED);
                    instanceMetaDataRepository.save(instanceMetaData);
                    nodesRemoved++;
                }
            }
            int nodeCount = instanceGroup.getNodeCount() - nodesRemoved;
            LOGGER.info("Successfully terminated metadata of instances '{}' in stack.", instanceIds);
            eventService.fireCloudbreakEvent(stack.getId(), BillingStatus.BILLING_CHANGED.name(),
                    cloudbreakMessagesService.getMessage(Msg.STACK_SCALING_BILLING_CHANGED.code()));
            return nodeCount;
        });
    }

    public Map<String, String> getUnusedInstanceIds(String instanceGroupName, Stack stack) {
        return getUnusedInstanceIds(instanceGroupName, Integer.MAX_VALUE, stack);
    }

    public Map<String, String> getUnusedInstanceIds(String instanceGroupName, Integer scalingAdjustment, Stack stack) {
        InstanceGroup instanceGroup = stack.getInstanceGroupByInstanceGroupName(instanceGroupName);
        List<InstanceMetaData> unattachedInstanceMetaDatas = new ArrayList<>(instanceGroup.getUnattachedInstanceMetaDataSet());

        return unattachedInstanceMetaDatas.stream()
                .filter(instanceMetaData -> instanceMetaData.getInstanceId() != null && instanceMetaData.getDiscoveryFQDN() != null)
                .sorted(Comparator.comparing(InstanceMetaData::getStartDate))
                .limit(scalingAdjustment)
                .collect(Collectors.toMap(InstanceMetaData::getInstanceId, InstanceMetaData::getDiscoveryFQDN));
    }

    private enum Msg {
        STACK_SCALING_HOST_DELETED("stack.scaling.host.deleted"),
        STACK_SCALING_HOST_DELETE_FAILED("stack.scaling.host.delete.failed"),
        STACK_SCALING_HOST_NOT_FOUND("stack.scaling.host.not.found"),
        STACK_SCALING_BILLING_CHANGED("stack.scaling.billing.changed");

        private final String code;

        Msg(String msgCode) {
            code = msgCode;
        }

        public String code() {
            return code;
        }
    }

}
