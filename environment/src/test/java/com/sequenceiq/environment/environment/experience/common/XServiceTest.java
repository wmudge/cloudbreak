package com.sequenceiq.environment.environment.experience.common;

import com.sequenceiq.environment.environment.domain.Environment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class XServiceTest {

    private static final String EXPERIENCE_PROTOCOL = "https";

    private static final String PATH_POSTFIX = "/cp-internal/environment/{crn}";

    private static final String TENANT = "someTenantValue";

    private static final String ENV_CRN = "someEnvCrnValue";

    private static final String XP_PORT = "9999";

    private static final String XP_PREFIX = "127.0.0.1";

    private static final String XP_INFIX = "/somexp/api/v3";

    @Mock
    private CommonExperienceConnectorService mockExperienceConnectorService;

    @Mock
    private XPServices mockExperienceProvider;

    @Mock
    private CommonExperienceValidator mockExperienceValidator;

    @Mock
    private Environment mockEnvironment;

   @Mock
   private CommonExperience mockCommonExperience;

    private XService underTest;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockEnvironment.getResourceCrn()).thenReturn(ENV_CRN);
        when(mockCommonExperience.getPathInfix()).thenReturn(XP_INFIX);
        when(mockCommonExperience.getPathPrefix()).thenReturn(XP_PREFIX);
        when(mockCommonExperience.getPort()).thenReturn(XP_PORT);
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenEnvironmentCrnIsNullThenIllegalArgumentExceptionComes() {
        when(mockEnvironment.getResourceCrn()).thenReturn(null);

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT));

        assertNotNull(exception);
        assertEquals("Unable to check environment - experience relation, since the " +
                "given environment crn is null or empty!", exception.getMessage());
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenEnvironmentCrnIsEmptyThenIllegalArgumentExceptionComes() {
        when(mockEnvironment.getResourceCrn()).thenReturn("");

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT));

        assertNotNull(exception);
        assertEquals("Unable to check environment - experience relation, since the " +
                "given environment crn is null or empty!", exception.getMessage());
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenNoConfiguredExperienceExistsThenNoXpConnectorServiceCallHappens() {
        when(mockExperienceProvider.getExperiences()).thenReturn(Collections.emptyMap());

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        verify(mockExperienceConnectorService, never()).getWorkspaceNamesConnectedToEnv(any(), any());
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenNoConfiguredExperienceExistsThenFalseReturns() {
        when(mockExperienceProvider.getExperiences()).thenReturn(Collections.emptyMap());

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        boolean result = underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        assertFalse(result);
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenHaveConfiguredExperienceButItsNotProperlyFilledThenNoXpConnectorServiceCallHappens() {
        when(mockExperienceProvider.getExperiences()).thenReturn(Map.of("AWESOME_XP", mockCommonExperience));
        when(mockExperienceValidator.isExperienceFilled(mockCommonExperience)).thenReturn(false);

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        verify(mockExperienceConnectorService, never()).getWorkspaceNamesConnectedToEnv(any(), any());
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenHaveConfiguredExperienceButItsNotProperlyFilledThenFalseReturns() {
        when(mockExperienceProvider.getExperiences()).thenReturn(Map.of("AWESOME_XP", mockCommonExperience));
        when(mockExperienceValidator.isExperienceFilled(mockCommonExperience)).thenReturn(false);

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        boolean result = underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        assertFalse(result);
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenExperienceIsConfiguredThenPathToExperienceShouldBeCombindedProperly() {
        String expectedPath = EXPERIENCE_PROTOCOL + "://" + XP_PREFIX + ":" + XP_PORT + XP_INFIX + PATH_POSTFIX;
        when(mockExperienceProvider.getExperiences()).thenReturn(Map.of("AWESOME_XP", mockCommonExperience));
        when(mockExperienceValidator.isExperienceFilled(mockCommonExperience)).thenReturn(true);

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        verify(mockExperienceConnectorService, times(1)).getWorkspaceNamesConnectedToEnv(any(), any());
        verify(mockExperienceConnectorService, times(1)).getWorkspaceNamesConnectedToEnv(expectedPath, ENV_CRN);
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenExperienceIsConfiguredButHasNoActiveWorkspaceForEnvThenFalseReturns() {
        when(mockExperienceProvider.getExperiences()).thenReturn(Map.of("AWESOME_XP", mockCommonExperience));
        when(mockExperienceValidator.isExperienceFilled(mockCommonExperience)).thenReturn(true);

        when(mockExperienceConnectorService.getWorkspaceNamesConnectedToEnv(any(), eq(ENV_CRN))).thenReturn(Collections.emptySet());

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        boolean result = underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        assertFalse(result);
    }

    @Test
    void testHasExistingClusterForEnvironmentWhenExperienceIsConfiguredAndHasActiveWorkspaceForEnvThentrueReturns() {
        when(mockExperienceProvider.getExperiences()).thenReturn(Map.of("AWESOME_XP", mockCommonExperience));
        when(mockExperienceValidator.isExperienceFilled(mockCommonExperience)).thenReturn(true);

        when(mockExperienceConnectorService.getWorkspaceNamesConnectedToEnv(any(), eq(ENV_CRN))).thenReturn(Set.of("SomeConnectedXP"));

        underTest = new XService(EXPERIENCE_PROTOCOL, PATH_POSTFIX, mockExperienceConnectorService, mockExperienceProvider, mockExperienceValidator);

        boolean result = underTest.hasExistingClusterForEnvironment(mockEnvironment, TENANT);

        assertTrue(result);
    }

}