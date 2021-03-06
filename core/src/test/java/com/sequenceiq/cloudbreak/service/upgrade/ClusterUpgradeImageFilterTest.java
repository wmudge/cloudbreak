package com.sequenceiq.cloudbreak.service.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.sequenceiq.cloudbreak.cloud.model.catalog.Image;
import com.sequenceiq.cloudbreak.cloud.model.catalog.Images;
import com.sequenceiq.cloudbreak.cloud.model.catalog.Versions;
import com.sequenceiq.cloudbreak.service.image.VersionBasedImageFilter;

@RunWith(MockitoJUnitRunner.class)
public class ClusterUpgradeImageFilterTest {

    private static final String CLOUD_PLATFORM = "aws";

    private static final String OS_TYPE = "redhat7";

    private static final String OS = "centos7";

    private static final String V_7_0_3 = "7.0.3";

    private static final String V_7_0_2 = "7.0.2";

    private static final String CMF_VERSION = "2.0.0.0-121";

    private static final String CSP_VERSION = "3.0.0.0-103";

    private static final String SALT_VERSION = "2017.7.5";

    private static final String CURRENT_IMAGE_ID = "f58c5f97-4609-4b47-6498-1c1bc6a4501c";

    private static final String IMAGE_ID = "f7f3fc53-b8a6-4152-70ac-7059ec8f8443";

    private static final Map<String, String> IMAGE_MAP = Collections.emptyMap();

    private static final long CREATED = 100000L;

    private static final long CREATED_LARGER = 200000L;

    private static final long CREATED_SMALLER = 1L;

    private static final String DATE = "2020-05-16";

    @InjectMocks
    private ClusterUpgradeImageFilter underTest;

    @Mock
    private UpgradePermissionProvider upgradePermissionProvider;

    @Mock
    private VersionBasedImageFilter versionBasedImageFilter;

    @Mock
    private Versions supportedCbVersions;

    private Image currentImage;

    private Image properImage;

    private boolean lockComponents;

    @Before
    public void before() {
        currentImage = createCurrentImage();
        properImage = createProperImage();
    }

    @Test
    public void testFilterShouldReturnTheAvailableImage() {
        List<Image> properImages = List.of(properImage);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, properImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, properImages)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(properImage), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(properImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getReason(), actual.getAvailableImages().getCdhImages().contains(this.properImage));
        assertEquals(1, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnTheProperImageWhenTheCloudPlatformDoesNotMatches() {
        Image availableImages = createImageWithDifferentPlatform();
        List<Image> allImage = List.of(availableImages, properImage);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), any(), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getReason(), actual.getAvailableImages().getCdhImages().contains(properImage));
        assertEquals(1, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheCloudPlatformIsNotMatches() {
        Image imageWithDifferentPlatform = createImageWithDifferentPlatform();
        List<Image> allImage = List.of(imageWithDifferentPlatform);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(imageWithDifferentPlatform), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no eligible images to upgrade for aws cloud platform.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheOsIsNotMatches() {
        Image imageWithDifferentOs = createImageWithDifferentOs();
        List<Image> allImage = List.of(imageWithDifferentOs);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(imageWithDifferentOs), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no eligible images to upgrade with the same OS version.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheImageIsCreatedBeforeAndLockComponents() {
        List<Image> allImage = List.of(createImageWithSmallerCreatedNumber());
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");
        lockComponents = true;

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no newer images available than " + DATE + ".", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheImageIsCreatedBefore() {
        List<Image> allImage = List.of(createImageWithSmallerCreatedNumber());
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(any(), any(), any(), any())).thenReturn(true);
        when(upgradePermissionProvider.permitSaltUpgrade(any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertEquals(1, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheVersioningIsNotSupported() {
        List<Image> allImage = List.of(createImageWithDifferentStackVersioning());
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no eligible images with supported Cloudera Manager or CDP version.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenCmVersionIsNotAvailable() {
        List<Image> allImage = List.of(createImageWithoutCmVersion());
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, allImage, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, allImage)).thenReturn(imageFilterResult);

        ImageFilterResult actual = underTest.filter(allImage, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no eligible images to upgrade available with Cloudera Manager packages.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnTheProperImageWhenTheCmVersionIsNotGreater() {
        Image lowerCmImage = createImageWithLowerCmVersion();
        Image lowerCmAndCdpImage = createImageWithLowerCmAndCdpVersion();
        List<Image> availableImages = List.of(properImage, lowerCmImage, lowerCmAndCdpImage);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);

        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(lowerCmAndCdpImage), any(), any())).thenReturn(false);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(lowerCmImage), any(), any())).thenReturn(true);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(properImage), any(), any())).thenReturn(true);
        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().contains(properImage));
        assertTrue(actual.getAvailableImages().getCdhImages().contains(lowerCmImage));
        assertEquals(2, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheStackVersionIsNotGreater() {
        List<Image> availableImages = List.of(createImageWithSameStackAndCmVersion());
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There is no proper Cloudera Manager or CDP version to upgrade.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheStackVersionIsEqualAndTheBuildNumberIsGreater() {
        Image imageWithSameStackAndCmVersion = createImageWithSameStackAndCmVersion();
        List<Image> availableImages = List.of(imageWithSameStackAndCmVersion);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);
        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(imageWithSameStackAndCmVersion), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getReason(), actual.getAvailableImages().getCdhImages().contains(imageWithSameStackAndCmVersion));
        assertEquals(1, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnTheProperImageWhenTheSaltVersionDoesNotMatch() {
        Image differentSaltVersionImage = createImageWithDifferentSaltVersion();
        List<Image> availableImages = List.of(properImage, differentSaltVersionImage);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(differentSaltVersionImage), any(), any())).thenReturn(true);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(properImage), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().contains(properImage));
        assertEquals(1, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenTheSaltVersionDoesNotMatch() {
        Image imageWithDifferentSaltVersion = createImageWithDifferentSaltVersion();
        List<Image> availableImages = List.of(imageWithDifferentSaltVersion);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(imageWithDifferentSaltVersion), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no images with compatible Salt version.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnTheProperImageWhenTheCurrentImageIsAlsoAvailable() {
        Image imageWithSameId = createImageWithCurrentImageId();
        List<Image> availableImages = List.of(properImage, imageWithSameId);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);
        when(upgradePermissionProvider.permitCmAndStackUpgrade(eq(currentImage), eq(properImage), any(), any())).thenReturn(true);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().contains(properImage));
        assertEquals(1, actual.getAvailableImages().getCdhImages().size());
    }

    @Test
    public void testFilterShouldReturnReasonMessageWhenOnlyTheCurrentImageIsAvailable() {
        List<Image> availableImages = List.of(createImageWithCurrentImageId());
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertTrue(actual.getAvailableImages().getCdhImages().isEmpty());
        assertEquals("There are no newer compatible images available.", actual.getReason());
    }

    @Test
    public void testFilterShouldReturnOnlyOneImageWhenLockComponentsIsSet() {
        Image imageWithSameStackAndCmVersion1 = createImageWithSameStackAndCmVersion();
        Image imageWithSameStackAndCmVersion2 = createImageWithSameStackAndCmVersion();
        Image imageWithDifferentStackVersioning = createImageWithDifferentStackVersioning();
        List<Image> availableImages = List.of(imageWithSameStackAndCmVersion1, imageWithSameStackAndCmVersion2, imageWithDifferentStackVersioning);
        ImageFilterResult imageFilterResult = new ImageFilterResult(new Images(null, availableImages, null), "");
        lockComponents = true;

        when(versionBasedImageFilter.getCdhImagesForCbVersion(supportedCbVersions, availableImages)).thenReturn(imageFilterResult);
        when(upgradePermissionProvider.permitSaltUpgrade(SALT_VERSION, SALT_VERSION)).thenReturn(true);

        ImageFilterResult actual = underTest.filter(availableImages, supportedCbVersions, currentImage, CLOUD_PLATFORM, lockComponents);

        assertEquals(actual.getReason(), 2, actual.getAvailableImages().getCdhImages().size());
    }

    private Image createCurrentImage() {
        return new Image(DATE, CREATED, null, OS, CURRENT_IMAGE_ID, null, null,
                Map.of(CLOUD_PLATFORM, Collections.emptyMap()), null, OS_TYPE,
                createPackageVersions(V_7_0_2, V_7_0_2, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createProperImage() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null,
                Map.of(CLOUD_PLATFORM, Collections.emptyMap()), null, OS_TYPE,
                createPackageVersions(V_7_0_3, V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithDifferentPlatform() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null, Map.of("azure", IMAGE_MAP), null, OS_TYPE,
                createPackageVersions(V_7_0_3, V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithDifferentOs() {
        return new Image(null, CREATED_LARGER, null, "ubuntu", IMAGE_ID, null, null, Map.of(CLOUD_PLATFORM, IMAGE_MAP), null, OS_TYPE,
                createPackageVersions(V_7_0_3, V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithSmallerCreatedNumber() {
        return new Image(null, CREATED_SMALLER, null, OS, IMAGE_ID, null, null, Map.of(CLOUD_PLATFORM, IMAGE_MAP), null, OS_TYPE,
                createPackageVersions(V_7_0_3, V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithLowerCmVersion() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null,
                Map.of(CLOUD_PLATFORM, IMAGE_MAP), null, OS_TYPE, createPackageVersions(V_7_0_2, V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithSameStackAndCmVersion() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null,
                Map.of(CLOUD_PLATFORM, IMAGE_MAP), null, OS_TYPE, createPackageVersions(V_7_0_2, V_7_0_2, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithDifferentStackVersioning() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null,
                Map.of(CLOUD_PLATFORM, Collections.emptyMap()), null, OS_TYPE,
                createPackageVersions("7.x.0", V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION),
                null, null, null);
    }

    private Image createImageWithLowerCmAndCdpVersion() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null, Map.of(CLOUD_PLATFORM, IMAGE_MAP), null,
                OS_TYPE, createPackageVersions(V_7_0_2, V_7_0_2, CMF_VERSION, CSP_VERSION, SALT_VERSION), null, null, null);
    }

    private Image createImageWithoutCmVersion() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null, Map.of(CLOUD_PLATFORM, IMAGE_MAP), null,
                OS_TYPE, createPackageVersions("", V_7_0_2, CMF_VERSION, CSP_VERSION, SALT_VERSION), null, null, null);
    }

    private Image createImageWithDifferentSaltVersion() {
        return new Image(null, CREATED_LARGER, null, OS, IMAGE_ID, null, null, Map.of(CLOUD_PLATFORM, IMAGE_MAP), null,
                OS_TYPE, createPackageVersions(V_7_0_3, V_7_0_3, CMF_VERSION, CSP_VERSION, "2018.7.5"), null, null, null);
    }

    private Image createImageWithCurrentImageId() {
        return new Image(null, CREATED_LARGER, null, OS, CURRENT_IMAGE_ID, null, null, Map.of(CLOUD_PLATFORM, IMAGE_MAP),
                null, OS_TYPE, createPackageVersions(V_7_0_3, V_7_0_3, CMF_VERSION, CSP_VERSION, SALT_VERSION), null, null, null);
    }

    private Map<String, String> createPackageVersions(String cmVersion, String cdhVersion, String cfmVersion, String cspVersion, String saltVersion) {
        return Map.of(
                "cm", cmVersion,
                "stack", cdhVersion,
                "cfm", cfmVersion,
                "csp", cspVersion,
                "salt", saltVersion);
    }
}