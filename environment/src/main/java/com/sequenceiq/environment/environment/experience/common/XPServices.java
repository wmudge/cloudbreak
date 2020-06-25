package com.sequenceiq.environment.environment.experience.common;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@ConfigurationProperties("experience.scan.xpservices")
public class XPServices {

    private Map<String, CommonExperience> experiences = new HashMap<>();

    /**
     * @return the value of the internal map if it's not null. Otherwise an empty map will return.
     */
    public Map<String, CommonExperience> getExperiences() {
        return experiences != null ? experiences : new HashMap<>();
    }

    public void setExperiences(Map<String, CommonExperience> experiences) {
        this.experiences = experiences;
    }

}
