package com.sequenceiq.periscope.monitor.event;

import java.util.Set;

import org.springframework.context.ApplicationEvent;

import com.sequenceiq.periscope.domain.BaseAlert;

public class ScalingEvent extends ApplicationEvent {

    public ScalingEvent(BaseAlert alert) {
        this(Set.of(alert));
    }

    public ScalingEvent(Set<? extends BaseAlert> alerts) {
        super(alerts);
    }

    public Set<BaseAlert> getAlerts() {
        return (Set<BaseAlert>) getSource();
    }

}
