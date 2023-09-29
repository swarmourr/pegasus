/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container.MountPoint;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/** @author vahi */
public class ContainerTest {

    private TestSetup mTestSetup;

    public ContainerTest() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterClass
    public static void tearDownClass() {}

    @After
    public void tearDown() {}

    @Test
    public void testSingularityFileCVMFS() {
        this.testSingulartiy(
                "test",
                "test",
                "file:///cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:latest");
    }

    @Test
    public void testSingularityFileCVMFSTagVersion() {
        this.testSingulartiy(
                "test",
                "test",
                "file://localhost/cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:latest");
    }

    @Test
    public void testSingularityFileCVMFSTagVersionWithDot() {
        this.testSingulartiy(
                "test",
                "test",
                "file://localhost/cvmfs/singularity.opensciencegrid.org/pycbc/pycbc-el7:v1.13.0");
    }

    @Test
    public void testSingulartiyHUB() {
        this.testSingulartiy("test", "test.simg", "shub://pegasus-isi/montage-workflow-v2");
    }

    @Test
    public void testSingulartiyHTTPImg() {
        this.testSingulartiy(
                "test", "test.img", "http:///pegasus.isi.edu/images/singularity/centos-7.img");
    }

    @Test
    public void testSingulartiyHTTPSImg() {
        this.testSingulartiy(
                "test", "test.simg", "http:///pegasus.isi.edu/images/singularity/centos-7.simg");
    }

    @Test
    public void testSingulartiyHTTPSSif() {
        this.testSingulartiy(
                "salmonella_ice",
                "salmonella_ice.sif",
                "https://workflow.isi.edu/scratch/rynge/ffh-workflow_latest.sif");
    }

    @Test
    public void testSingulartiyHTTPSPostImg() {
        this.testSingulartiy(
                "test", "test.simg", "http://pegasus.isi.edu/container.php?rid=/centos-7.simg");
    }

    @Test
    public void testSingulartiyHTTPTar() {
        this.testSingulartiy(
                "test", "test.tar", "http:///pegasus.isi.edu/images/singularity/centos-7.tar");
    }

    @Test
    public void testSingulartiyHTTPTarGZ() {
        this.testSingulartiy(
                "test",
                "test.tar.gz",
                "http:///pegasus.isi.edu/images/singularity/centos-7.tar.gz");
    }

    @Test
    public void testSingulartiyHTTPTarBZ() {
        this.testSingulartiy(
                "test",
                "test.tar.bz2",
                "http:///pegasus.isi.edu/images/singularity/centos-7.tar.bz2");
    }

    @Test
    public void testSingulartiyHTTPCPIO() {
        this.testSingulartiy(
                "test", "test.cpio", "http:///pegasus.isi.edu/images/singularity/centos-7.cpio");
    }

    @Test
    public void testSingulartiyHTTPTarCPIO() {
        this.testSingulartiy(
                "test",
                "test.cpio.gz",
                "http:///pegasus.isi.edu/images/singularity/centos-7.cpio.gz");
    }

    @Test
    public void testSimpleMountPoint() {
        this.testMountPoint(
                "/scitech/shared/scratch-90-days/:/existing/data",
                "/scitech/shared/scratch-90-days/",
                "/existing/data",
                null);
    }

    @Test
    /** PM-1906 */
    public void testSimpleMountPointWithDotInName() {
        this.testMountPoint(
                "/shared/pegasus-5.0.6dev/:/shared/pegasus-5.0.6dev/",
                "/shared/pegasus-5.0.6dev/",
                "/shared/pegasus-5.0.6dev/",
                null);
    }

    @Test
    public void testSimpleMountPointWithOptions() {
        this.testMountPoint(
                "/scitech/shared/scratch-90-days/:/existing/data:ro",
                "/scitech/shared/scratch-90-days/",
                "/existing/data",
                "ro");
    }

    @Test
    public void testSimpleMountPointWithShellVariable() {
        this.testMountPoint(
                "/scitech/shared/scratch-90-days/${TEST_NAME}:/existing/data:ro",
                "/scitech/shared/scratch-90-days/${TEST_NAME}",
                "/existing/data",
                "ro");
    }

    public void testMountPoint(
            String actual, String expectedSource, String expectedDest, String options) {
        MountPoint mp = new MountPoint(actual);
        assertEquals(expectedSource, mp.getSourceDirectory());
        assertEquals(expectedDest, mp.getDestinationDirectory());
        assertEquals(options, mp.getMountOptions());
    }

    public void testSingulartiy(String name, String expectedLFN, String url) {
        Container c = new Container(name);
        c.setType(Container.TYPE.singularity);
        String lfn = c.computeLFN(new PegasusURL(url));
        assertEquals(expectedLFN, lfn);
    }

    @Test
    public void deserializeContainer() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "name: centos-pegasus\n"
                        + "type: docker\n"
                        + "image: docker:///rynge/montage:latest\n"
                        + "mounts: \n"
                        + "  - /Volumes/Work/lfs1:/shared-data/:ro\n"
                        + "  - /Volumes/Work/lfs12:/shared-data1/:ro\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: /opt/java/1.6";
        Container c = mapper.readValue(test, Container.class);
        assertNotNull(c);
        assertEquals(Container.TYPE.docker, c.getType());
        assertEquals("docker:///rynge/montage:latest", c.getImageURL().getURL());
        assertFalse(c.bypassStaging());
        assertEquals(2, c.getMountPoints().size());
        assertThat(
                c.getMountPoints(), hasItem(new MountPoint("/Volumes/Work/lfs1:/shared-data/:ro")));
        assertThat(
                c.getMountPoints(), hasItem(new MountPoint("/Volumes/Work/lfs1:/shared-data/:ro")));

        List<Profile> profiles = c.getProfiles("env");
        assertThat(profiles, hasItem(new Profile("env", "JAVA_HOME", "/opt/java/1.6")));
    }

    @Test
    public void deserializeContainerWithChecksum() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "name: centos-pegasus\n"
                        + "type: docker\n"
                        + "image: docker:///rynge/montage:latest\n"
                        + "checksum:\n"
                        + "  sha256: \"a08d9d7769cffb96a910a4b6c2be7bfd85d461c9\"\n";

        Container c = mapper.readValue(test, Container.class);
        assertNotNull(c);
        assertEquals(Container.TYPE.docker, c.getType());
        assertEquals("docker:///rynge/montage:latest", c.getImageURL().getURL());
        assertFalse(c.bypassStaging());
        List<Profile> profiles = c.getProfiles("metadata");
        assertThat(
                profiles, hasItem(new Profile("metadata", Metadata.CHECKSUM_TYPE_KEY, "sha256")));
        assertThat(
                profiles,
                hasItem(
                        new Profile(
                                "metadata",
                                Metadata.CHECKSUM_VALUE_KEY,
                                "a08d9d7769cffb96a910a4b6c2be7bfd85d461c9")));
    }

    @Test
    public void deserializeContainerWithBypass() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "name: centos-pegasus\n"
                        + "type: docker\n"
                        + "image: docker:///rynge/montage:latest\n"
                        + "bypass: true\n";

        Container c = mapper.readValue(test, Container.class);
        assertNotNull(c);
        assertEquals(Container.TYPE.docker, c.getType());
        assertEquals("docker:///rynge/montage:latest", c.getImageURL().getURL());
        assertTrue(c.bypassStaging());
    }

    @Test
    public void deserializeContainerWithChecksumAndProfiles() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "name: centos-pegasus\n"
                        + "type: docker\n"
                        + "image: docker:///rynge/montage:latest\n"
                        + "checksum:\n"
                        + "  sha256: \"a08d9d7769cffb96a910a4b6c2be7bfd85d461c9\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: /opt/java/1.6";

        Container c = mapper.readValue(test, Container.class);
        assertNotNull(c);
        assertEquals(Container.TYPE.docker, c.getType());
        assertEquals("docker:///rynge/montage:latest", c.getImageURL().getURL());
        assertFalse(c.bypassStaging());
        List<Profile> profiles = c.getProfiles("metadata");
        assertThat(
                profiles, hasItem(new Profile("metadata", Metadata.CHECKSUM_TYPE_KEY, "sha256")));
        assertThat(
                profiles,
                hasItem(
                        new Profile(
                                "metadata",
                                Metadata.CHECKSUM_VALUE_KEY,
                                "a08d9d7769cffb96a910a4b6c2be7bfd85d461c9")));

        List<Profile> p = c.getProfiles("env");
        assertThat(p, hasItem(new Profile("env", "JAVA_HOME", "/opt/java/1.6")));
    }

    @Test
    public void serializeContainer() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        Container c = new Container();
        c.setName("centos-pegasus");
        c.setImageSite("dockerhub");
        c.setImageURL("docker:///rynge/montage:latest");
        c.setType(Container.TYPE.docker);
        c.addMountPoint("/Volumes/Work/lfs1:/shared-data/:ro");
        c.addMountPoint("/Volumes/Work/lfs12:/shared-data1/:ro");
        c.addProfile(new Profile(Profiles.NAMESPACES.env.toString(), "JAVA_HOME", "/opt/java/1.6"));

        String expected =
                "---\n"
                        + "name: \"centos-pegasus\"\n"
                        + "type: \"docker\"\n"
                        + "image: \"docker:///rynge/montage:latest\"\n"
                        + "image.site: \"dockerhub\"\n"
                        + "bypass: false\n"
                        + "mounts:\n"
                        + " - \"/Volumes/Work/lfs1:/shared-data/:ro\"\n"
                        + " - \"/Volumes/Work/lfs12:/shared-data1/:ro\"\n"
                        + "profiles:\n"
                        + "  env:\n"
                        + "    JAVA_HOME: \"/opt/java/1.6\"\n";

        String actual = mapper.writeValueAsString(c);
        assertEquals(expected, actual);
    }

    @Test
    public void serializeContainerWithBypass() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        Container c = new Container();
        c.setName("centos-pegasus");
        c.setImageSite("osg");
        c.setImageURL("library:///rynge/montage:latest");
        c.setType(Container.TYPE.singularity);
        c.setForBypassStaging();
        String expected =
                "---\n"
                        + "name: \"centos-pegasus\"\n"
                        + "type: \"singularity\"\n"
                        + "image: \"library:///rynge/montage:latest\"\n"
                        + "image.site: \"osg\"\n"
                        + "bypass: true\n";

        String actual = mapper.writeValueAsString(c);
        assertEquals(expected, actual);
    }

    @Test
    public void serializeContainerChecksum() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        Container c = new Container();
        c.setName("centos-pegasus");
        c.setImageSite("dockerhub");
        c.setImageURL("docker:///rynge/montage:latest");
        c.setType(Container.TYPE.docker);
        c.addProfile(
                new Profile(
                        Profiles.NAMESPACES.metadata.toString(),
                        Metadata.CHECKSUM_TYPE_KEY,
                        "sha256"));
        c.addProfile(
                new Profile(
                        Profiles.NAMESPACES.metadata.toString(),
                        Metadata.CHECKSUM_VALUE_KEY,
                        "dsadsadsa093232"));
        String expected =
                "---\n"
                        + "name: \"centos-pegasus\"\n"
                        + "type: \"docker\"\n"
                        + "image: \"docker:///rynge/montage:latest\"\n"
                        + "image.site: \"dockerhub\"\n"
                        + "bypass: false\n"
                        + "checksum:\n"
                        + "  sha256: \"dsadsadsa093232\"\n";

        String actual = mapper.writeValueAsString(c);
        // System.err.println(actual);
        assertEquals(expected, actual);
    }
}
