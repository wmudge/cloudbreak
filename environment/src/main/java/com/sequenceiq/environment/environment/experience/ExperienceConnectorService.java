package com.sequenceiq.environment.environment.experience;

import com.sequenceiq.cloudbreak.api.endpoint.v4.userprofile.UserProfileV4Endpoint;
import com.sequenceiq.cloudbreak.util.NullUtil;
import com.sequenceiq.environment.environment.domain.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ExperienceConnectorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExperienceConnectorService.class);

    private final List<Experience> experiences;

    private final UserProfileV4Endpoint userProfileV4Endpoint;

    private final boolean scanEnabled;

    public ExperienceConnectorService(@Value("${experience.scan.enabled}") boolean scanEnabled,
                                      List<Experience> experiences,
                                      UserProfileV4Endpoint userProfileV4Endpoint) {
        this.experiences = experiences;
        this.scanEnabled = scanEnabled;
        this.userProfileV4Endpoint = userProfileV4Endpoint;
    }

    public long getConnectedExperienceQuantity(Environment environment) {
        NullUtil.throwIfNull(environment, () -> new IllegalArgumentException("environment should not be null!"));
        if (scanEnabled && experiences.size() > 0) {
            LOGGER.debug("Collecting connected experiences for environment: {}", environment.getName());
            String tenant = userProfileV4Endpoint.get().getTenant();
            return experiences
                    .stream()
                    .filter(experience -> experience.hasExistingClusterForEnvironment(environment, tenant))
                    .count();
        }
        LOGGER.info("Scanning experience(s) has disabled, which means the returning amount of connected experiences may not represent the reality!");
        return 0L;
    }

}
