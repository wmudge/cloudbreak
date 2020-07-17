package com.sequenceiq.periscope.monitor.evaluator;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.response.StackV4Response;
import com.sequenceiq.cloudbreak.logger.MDCBuilder;
import com.sequenceiq.periscope.api.model.ScalingStatus;
import com.sequenceiq.periscope.domain.BaseAlert;
import com.sequenceiq.periscope.domain.Cluster;
import com.sequenceiq.periscope.domain.ScalingPolicy;
import com.sequenceiq.periscope.domain.TimeAlert;
import com.sequenceiq.periscope.model.yarn.YarnScalingServiceV1Response;
import com.sequenceiq.periscope.monitor.MonitorUpdateRate;
import com.sequenceiq.periscope.monitor.client.YarnMetricsClient;
import com.sequenceiq.periscope.monitor.context.ClusterIdEvaluatorContext;
import com.sequenceiq.periscope.monitor.context.EvaluatorContext;
import com.sequenceiq.periscope.monitor.evaluator.load.YarnResponseUtils;
import com.sequenceiq.periscope.monitor.event.ScalingEvent;
import com.sequenceiq.periscope.monitor.handler.CloudbreakCommunicator;
import com.sequenceiq.periscope.repository.TimeAlertRepository;
import com.sequenceiq.periscope.service.ClusterService;
import com.sequenceiq.periscope.service.DateService;
import com.sequenceiq.periscope.service.HistoryService;
import com.sequenceiq.periscope.utils.StackResponseUtils;

@Component("CronTimeEvaluator")
@Scope("prototype")
public class CronTimeEvaluator extends EvaluatorExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CronTimeEvaluator.class);

    private static final String EVALUATOR_NAME = CronTimeEvaluator.class.getName();

    @Inject
    private TimeAlertRepository alertRepository;

    @Inject
    private ClusterService clusterService;

    @Inject
    private DateService dateService;

    @Inject
    private HistoryService historyService;

    @Inject
    private EventPublisher eventPublisher;

    @Inject
    private CloudbreakCommunicator cloudbreakCommunicator;

    @Inject
    private YarnMetricsClient yarnMetricsClient;

    @Inject
    private YarnResponseUtils yarnResponseUtils;

    @Inject
    private StackResponseUtils stackResponseUtils;

    @Inject
    private ScalingPolicyTargetCalculator scalingPolicyTargetCalculator;

    private long clusterId;

    @Override
    public void setContext(EvaluatorContext context) {
        clusterId = (long) context.getData();
    }

    @Override
    @Nonnull
    public EvaluatorContext getContext() {
        return new ClusterIdEvaluatorContext(clusterId);
    }

    @Override
    public String getName() {
        return EVALUATOR_NAME;
    }

    private boolean isTrigger(TimeAlert alert) {
        return dateService.isTrigger(alert, MonitorUpdateRate.CRON_UPDATE_RATE_IN_MILLIS);
    }

    private boolean isTrigger(TimeAlert alert, ZonedDateTime zdt) {
        return dateService.isTrigger(alert, MonitorUpdateRate.CRON_UPDATE_RATE_IN_MILLIS, zdt);
    }

    private boolean isPolicyAttached(BaseAlert alert) {
        return alert.getScalingPolicy() != null;
    }

    @Override
    public void execute() {
        long start = System.currentTimeMillis();
        Cluster cluster = clusterService.findById(clusterId);
        MDCBuilder.buildMdcContext(cluster);
        publishIfNeeded(alertRepository.findAllByCluster(clusterId));
        LOGGER.debug("Finished cronTimeEvaluator for cluster {} in {} ms", cluster.getStackCrn(), System.currentTimeMillis() - start);
    }

    protected void publishIfNeeded(List<TimeAlert> alerts) {
        TimeAlert triggeredAlert = null;
        for (TimeAlert alert : alerts) {
            boolean alertTriggerable = isTrigger(alert);
            if (isPolicyAttached(alert) && alertTriggerable && null == triggeredAlert) {
                publish(alert);
                triggeredAlert = alert;
            } else if (alertTriggerable && triggeredAlert != null) {
                historyService.createEntry(ScalingStatus.TRIGGER_FAILED, String.format(
                        "Autoscaling Schedule '%s' overlaps with '%s'.", alert.getName(), triggeredAlert.getName()),
                        alert.getCluster());
            }
        }
    }

    public void publishIfNeeded(Map<TimeAlert, ZonedDateTime> alerts) {
        for (Entry<TimeAlert, ZonedDateTime> alertEntry : alerts.entrySet()) {
            TimeAlert alert = alertEntry.getKey();
            if (isPolicyAttached(alert) && isTrigger(alert, alertEntry.getValue())) {
                publish(alert);
                break;
            }
        }
    }

    private void publish(TimeAlert alert) {
        ScalingEvent event = new ScalingEvent(alert);

        int hostGroupNodeCount = getHostGroupNodeCount(alert.getCluster(), alert.getScalingPolicy());
        int desiredNodeCount = scalingPolicyTargetCalculator.getDesiredNodeCount(event, hostGroupNodeCount);
        int targetNodeCount = desiredNodeCount - hostGroupNodeCount;

        event.setHostGroupNodeCount(hostGroupNodeCount);
        event.setDesiredHostGroupNodeCount(desiredNodeCount);
        if (targetNodeCount < 0) {
            populateDecommissionCandidates(event, alert.getCluster(), alert.getScalingPolicy(), -targetNodeCount);
        }

        eventPublisher.publishEvent(event);
        LOGGER.debug("Time alert '{}' triggered  for cluster '{}'", alert.getName(), alert.getCluster().getStackCrn());
    }

    private void populateDecommissionCandidates(ScalingEvent event, Cluster cluster, ScalingPolicy policy, int mandatoryDownScaleCount) {
        try {
            StackV4Response stackV4Response = cloudbreakCommunicator.getByCrn(cluster.getStackCrn());
            YarnScalingServiceV1Response yarnResponse = yarnMetricsClient.getYarnMetricsForCluster(cluster,
                    stackV4Response, policy.getHostGroup(), Optional.of(mandatoryDownScaleCount));
            Map<String, String> hostFqdnsToInstanceId = stackResponseUtils.getCloudInstanceIdsForHostGroup(stackV4Response, policy.getHostGroup());

            List<String> decommissionNodes = yarnResponseUtils.getYarnRecommendedDecommissionHostsForHostGroup(cluster.getStackCrn(), yarnResponse,
                    hostFqdnsToInstanceId, mandatoryDownScaleCount, Optional.of(mandatoryDownScaleCount));
            event.setDecommissionNodeIds(decommissionNodes);
        } catch (Exception ex) {
            LOGGER.error("Error retrieving decommission candidates for  policy '{}', adjustment type '{}', cluster '{}'",
                    policy.getName(), policy.getAdjustmentType(), cluster.getStackCrn(), ex);
        }
    }

    private Integer getHostGroupNodeCount(Cluster cluster, ScalingPolicy policy) {
        StackV4Response stackV4Response = cloudbreakCommunicator.getByCrn(cluster.getStackCrn());
        return stackResponseUtils.getNodeCountForHostGroup(stackV4Response, policy.getHostGroup());
    }
}