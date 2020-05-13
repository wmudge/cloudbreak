package com.sequenceiq.environment.environment.experience;

import com.sequenceiq.environment.environment.domain.Environment;

public interface Experience {

    boolean hasExistingClusterForEnvironment(Environment environment);

}
