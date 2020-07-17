package com.sequenceiq.periscope.monitor.evaluator;

import static java.lang.Math.ceil;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.sequenceiq.periscope.domain.ScalingPolicy;
import com.sequenceiq.periscope.monitor.event.ScalingEvent;
import com.sequenceiq.periscope.monitor.handler.CloudbreakCommunicator;
import com.sequenceiq.periscope.utils.ClusterUtils;
import com.sequenceiq.periscope.utils.StackResponseUtils;

@Component
public class ScalingPolicyTargetCalculator {

    @Inject
    private StackResponseUtils stackResponseUtils;

    @Inject
    private CloudbreakCommunicator cloudbreakCommunicator;

    public Integer getDesiredNodeCount(ScalingEvent event, int hostGroupNodeCount) {
        ScalingPolicy policy = event.getAlert().getScalingPolicy();
        int scalingAdjustment = policy.getScalingAdjustment();
        int desiredHostGroupNodeCount;
        switch (policy.getAdjustmentType()) {
            case NODE_COUNT:
                desiredHostGroupNodeCount = hostGroupNodeCount + scalingAdjustment;
                break;
            case PERCENTAGE:
                desiredHostGroupNodeCount = hostGroupNodeCount
                        + (int) (ceil(hostGroupNodeCount * ((double) scalingAdjustment / ClusterUtils.MAX_CAPACITY)));
                break;
            case EXACT:
                desiredHostGroupNodeCount = policy.getScalingAdjustment();
                break;
            default:
                desiredHostGroupNodeCount = hostGroupNodeCount;
        }
        int minSize = ScalingConstants.DEFAULT_HOSTGROUP_MIN_SIZE;
        int maxSize = ScalingConstants.DEFAULT_HOSTGROUP_MAX_SIZE;

        return desiredHostGroupNodeCount < minSize ? minSize : desiredHostGroupNodeCount > maxSize ? maxSize : desiredHostGroupNodeCount;
    }
}