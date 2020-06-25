package com.sequenceiq.environment.environment.experience;

import com.sequenceiq.cloudbreak.api.endpoint.v4.userprofile.UserProfileV4Endpoint;
import com.sequenceiq.environment.environment.domain.Environment;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ExperienceConnectorServiceTest {

    private static final boolean SCAN_ENABLED = true;

    private static final String TENANT = "someTenantValue";

    @Mock
    private Experience mockExperience;

    @Mock
    private UserProfileV4Endpoint mockUserProfileV4Endpoint;

    @Mock
    private Environment mockEnvironment;

    private ExperienceConnectorService underTest;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        underTest = new ExperienceConnectorService(SCAN_ENABLED, List.of(mockExperience), mockUserProfileV4Endpoint);
    }

    @Test
    void testWhenScanIsNotEnabledThenNoExperienceCallHappensAndZeroShouldReturn() {
        ExperienceConnectorService underTest = new ExperienceConnectorService(false, List.of(mockExperience), mockUserProfileV4Endpoint);
        long result = underTest.getConnectedExperienceQuantity(mockEnvironment);

        Assert.assertEquals(0L, result);
        verify(mockUserProfileV4Endpoint, never()).get();
        verify(mockExperience, never()).hasExistingClusterForEnvironment(any(Environment.class), any());
    }

    @Test
    void testWhenNoExperienceHasConfiguredThenZeroShouldReturn() {
        ExperienceConnectorService underTest = new ExperienceConnectorService(false, Collections.emptyList(), mockUserProfileV4Endpoint);
        long result = underTest.getConnectedExperienceQuantity(mockEnvironment);

        Assert.assertEquals(0L, result);
        verify(mockUserProfileV4Endpoint, never()).get();
    }

}