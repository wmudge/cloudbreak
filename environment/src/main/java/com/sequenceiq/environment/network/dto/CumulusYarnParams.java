package com.sequenceiq.environment.network.dto;

public class CumulusYarnParams {

    private String queue;

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public static final class CumulusYarnParamsBuilder {
        private String queue;

        private CumulusYarnParamsBuilder() {
        }

        public static CumulusYarnParamsBuilder aCumulusYarnParams() {
            return new CumulusYarnParamsBuilder();
        }

        public CumulusYarnParamsBuilder withQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public CumulusYarnParams build() {
            CumulusYarnParams cumulusYarnParams = new CumulusYarnParams();
            cumulusYarnParams.setQueue(queue);
            return cumulusYarnParams;
        }
    }
}
