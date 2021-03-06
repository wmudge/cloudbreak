package com.sequenceiq.datalake.flow.datalake.upgrade.event;

import com.sequenceiq.datalake.flow.SdxEvent;
import com.sequenceiq.datalake.flow.SdxFailedEvent;

public class DatalakeUpgradeFailedEvent extends SdxFailedEvent {

    public DatalakeUpgradeFailedEvent(Long sdxId, String userId, Exception exception) {
        super(sdxId, userId, exception);
    }

    public static DatalakeUpgradeFailedEvent from(SdxEvent event, Exception exception) {
        return new DatalakeUpgradeFailedEvent(event.getResourceId(), event.getUserId(), exception);
    }

    @Override
    public String selector() {
        return "DatalakeUpgradeFailedEvent";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DatalakeUpgradeFailedEvent{");
        sb.append("exception=").append(getException());
        sb.append('}');
        return sb.toString();
    }
}
