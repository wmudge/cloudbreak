package com.sequenceiq.cloudbreak.orchestrator.salt.runner;

import java.util.concurrent.Callable;

import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.orchestrator.OrchestratorBootstrap;
import com.sequenceiq.cloudbreak.orchestrator.OrchestratorBootstrapRunner;
import com.sequenceiq.cloudbreak.orchestrator.state.ExitCriteria;
import com.sequenceiq.cloudbreak.orchestrator.state.ExitCriteriaModel;

@Component
public class SaltRunner {

    private static final int SLEEP_TIME = 5000;

    @Value("${cb.max.salt.new.service.retry.onerror}")
    private int maxRetryOnError;

    @Value("${cb.max.salt.new.service.retry}")
    private int maxRetry;

    public Callable<Boolean> runner(OrchestratorBootstrap bootstrap, ExitCriteria exitCriteria, ExitCriteriaModel exitCriteriaModel, int maxRetry,
            boolean usingErrorCount) {
        return runner(bootstrap, exitCriteria, exitCriteriaModel, maxRetry, usingErrorCount, SLEEP_TIME);
    }

    public Callable<Boolean> runner(OrchestratorBootstrap bootstrap, ExitCriteria exitCriteria, ExitCriteriaModel exitCriteriaModel) {
        return runner(bootstrap, exitCriteria, exitCriteriaModel, maxRetry, false);
    }

    public Callable<Boolean> runner(OrchestratorBootstrap bootstrap, ExitCriteria exitCriteria, ExitCriteriaModel exitCriteriaModel, int sleepTime) {
        return runner(bootstrap, exitCriteria, exitCriteriaModel, maxRetry, false, sleepTime);
    }

    public Callable<Boolean> runner(OrchestratorBootstrap bootstrap, ExitCriteria exitCriteria, ExitCriteriaModel exitCriteriaModel, int maxRetry,
            boolean usingErrorCount, int sleepTime) {
        return new OrchestratorBootstrapRunner(bootstrap, exitCriteria, exitCriteriaModel, MDC.getCopyOfContextMap(), maxRetry, sleepTime,
                usingErrorCount ? maxRetryOnError : maxRetry);
    }
}
