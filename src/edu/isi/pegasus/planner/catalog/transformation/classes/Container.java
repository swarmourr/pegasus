/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.transformation.classes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.parser.ScannerException;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A container data class to use in the Transformation Catalog
 *
 * @author Karan Vahi
 */
@JsonDeserialize(using = Container.JsonDeserializer.class)
@JsonSerialize(using = Container.JsonSerializer.class)
public class Container implements Cloneable {

    /** The types of container supported. */
    public static enum TYPE {
        docker,
        singularity,
        shifter
    };

    /** Singularity is picky about extensions as it uses that for loading the container image */
    protected static Set<String> mSupportedSingularityExtensions = null;

    protected static Set<String> getsupportedSingularityExtensions() {
        if (mSupportedSingularityExtensions == null) {
            // from http://singularity.lbl.gov/user-guide#other-container-formats-supported
            mSupportedSingularityExtensions = new HashSet<String>();
            mSupportedSingularityExtensions.add(".img");
            mSupportedSingularityExtensions.add(".simg");
            mSupportedSingularityExtensions.add(".tar");
            mSupportedSingularityExtensions.add(".tar.gz");
            mSupportedSingularityExtensions.add(".tar.bz2");
            mSupportedSingularityExtensions.add(".cpio");
            mSupportedSingularityExtensions.add(".cpio.gz");
            mSupportedSingularityExtensions.add(".sif");
        }
        return mSupportedSingularityExtensions;
    }

    /** the container name assigned by user in the TC */
    protected String mName;

    /** the LFN used internally for determining destination basenames for staging. */
    protected String mLFN;

    /** Type of container to use */
    protected TYPE mType;

    /** URL to image in a docker hub or a url to an existing docker file exported as a tar file */
    protected PegasusURL mImageURL;

    /**
     * optional site attribute to tell pegasus which site tar file exists. useful for handling file
     * URL's correctly
     */
    protected String mImageSite;

    /** a url to an existing docker file to build container image from scratch */
    protected PegasusURL mDefinitionFileURL;

    /** Boolean flag to indicate whether to bypass staging in the executable via the staging site */
    private boolean mBypassStaging;

    /** The profiles associated with the site. */
    protected Profiles mProfiles;

    /** Collection of mount points to be mounted in the container. */
    protected Collection<MountPoint> mMountPoints;

    /** Default constructor */
    public Container() {
        mType = TYPE.docker;
        mName = null;
        mLFN = null;
        mImageURL = null;
        mDefinitionFileURL = null;
        mImageSite = null;
        mProfiles = new Profiles();
        mMountPoints = new HashSet<MountPoint>();
        mBypassStaging = false;
    }

    /**
     * Overloaded constructor
     *
     * @param name
     */
    public Container(String name) {
        this();
        mName = name;
        setLFN(name);
    }

    /**
     * Set the name/identifier for the container
     *
     * @param name
     */
    public void setName(String name) {
        mName = name;
        setLFN(name);
    }

    /**
     * The name of the container transformation.
     *
     * @return
     */
    public String getName() {
        return mName;
    }

    /**
     * Set the LFN for the container
     *
     * @param name
     */
    protected final void setLFN(String name) {
        mLFN = name;
    }

    /**
     * The name of the project
     *
     * @return
     */
    public String getLFN() {
        return mLFN;
    }

    /**
     * Compute LFN to be used based on the image URL for the container
     *
     * @param url
     * @return LFN
     */
    public String computeLFN(PegasusURL url) {
        String lfn = this.getName();
        String protocol = url.getProtocol();
        String path = url.getPath();
        if (this.mType.equals(Container.TYPE.singularity)) {

            String suffix = null;
            if (protocol.startsWith(PegasusURL.SINGULARITY_PROTOCOL_SCHEME)
                    || protocol.startsWith(PegasusURL.DOCKER_PROTOCOL_SCHEME)) {
                // default suffix while pulling from singularity hub is .simg
                suffix = ".simg";
            } else {
                // determine the suffix in the URL
                String basename = new File(path).getName();
                int dotIndex = basename.indexOf('.');

                // PM-1326 check if there is tag version specified
                if (basename.contains(":")) {
                    suffix = "";
                } else if (dotIndex != -1) {
                    suffix = basename.substring(dotIndex);
                    if (!Container.getsupportedSingularityExtensions().contains(suffix)) {
                        throw new RuntimeException(
                                "Invalid suffix "
                                        + suffix
                                        + " determined singularity image url "
                                        + url);
                    }
                } else {
                    // throw new RuntimeException( "Unable to compute singularity extension from " +
                    // basename + " for url " + url );
                    // PM-1313 cannot compute a suffix . just have it empty
                    suffix = "";
                }
            }
            lfn = lfn + suffix;
        } else if (this.mType.equals(Container.TYPE.shifter)) {
            StringBuilder sb = new StringBuilder();
            if (protocol != null
                    && !protocol.equalsIgnoreCase(PegasusURL.SHIFTER_PROTOCOL_SCHEME)) {
                sb.append(protocol).append(":");
            }
            if (path.startsWith(File.separator)) {
                sb.append(path.substring(1));
            } else {
                sb.append(path);
            }
            lfn = sb.toString();
        }
        return lfn;
    }

    /**
     * Set the image URL
     *
     * @param url
     */
    public void setImageURL(String url) {
        mImageURL = new PegasusURL(url);
        setLFN(computeLFN(mImageURL));
    }

    /**
     * Return the URL to the image
     *
     * @return
     */
    public PegasusURL getImageURL() {
        return mImageURL;
    }

    /**
     * Set image definition URL
     *
     * @param url
     */
    public void setImageDefinitionURL(String url) {
        mDefinitionFileURL = new PegasusURL(url);
    }

    /**
     * Return image defintion URL
     *
     * @return
     */
    public PegasusURL getImageDefinitionURL() {
        return mDefinitionFileURL;
    }

    /**
     * Set the site where image resides
     *
     * @param site
     */
    public void setImageSite(String site) {
        mImageSite = site;
    }

    /**
     * Return site with which image is associated
     *
     * @return
     */
    public String getImageSite() {
        return mImageSite;
    }

    /**
     * Allows you to add one profile at a time to the transformation.
     *
     * @param profiles profiles to be added.
     */
    public void addProfiles(Profiles profiles) {
        if (profiles != null) {
            if (this.mProfiles == null) {
                this.mProfiles = new Profiles();
            }
            this.mProfiles.addProfilesDirectly(profiles);
        }
    }

    /**
     * Allows you to add one profile at a time to the transformation.
     *
     * @param profile Profile A single profile consisting of mNamespace, key and value
     */
    public void addProfile(Profile profile) {
        if (profile != null) {
            if (this.mProfiles == null) {
                this.mProfiles = new Profiles();
            }
            // PM-826 allow multiple profiles with same key
            if (profile.getProfileNamespace().equalsIgnoreCase(Pegasus.NAMESPACE_NAME)) {
                this.mProfiles.addProfile(profile);
            } else {
                this.mProfiles.addProfileDirectly(profile);
            }
        }
    }

    /**
     * Returns the list of all profiles associated with the container.
     *
     * @return List Returns null if no profiles associated.
     */
    public Profiles getAllProfiles() {
        return this.mProfiles;
    }

    /**
     * Returns the list of profiles associated with the transformation.
     *
     * @return List Returns null if no profiles associated.
     */
    public List<Profile> getProfiles() {
        return (this.mProfiles == null) ? null : this.mProfiles.getProfiles();
    }

    /**
     * Returns the profiles for a particular Namespace.
     *
     * @param namespace String The mNamespace of the profile
     * @return List List of Profile objects. returns null if none are found.
     */
    public List<Profile> getProfiles(String namespace) {
        return (this.mProfiles == null) ? null : mProfiles.getProfiles(namespace);
    }

    public Profiles getProfilesObject() {
        return this.mProfiles;
    }

    /**
     * Allows you to add multiple profiles to the transformation.
     *
     * @param profiles List of Profile objects containing the profile information.
     */
    public void addProfiles(List profiles) {
        if (profiles != null) {
            if (this.mProfiles == null) {
                this.mProfiles = new Profiles();
            }
            this.mProfiles.addProfilesDirectly(profiles);
        }
    }

    /**
     * Adds a mount point
     *
     * @param mount point string as src-mp:dest-mp:options
     */
    public void addMountPoint(MountPoint mount) {
        this.mMountPoints.add(mount);
    }

    /**
     * Adds a mount point
     *
     * @param mount point string as src-mp:dest-mp:options
     */
    public void addMountPoint(String mount) {
        this.mMountPoints.add(new MountPoint(mount));
    }

    /**
     * Return a local file URL path in the container based on the mount points in the container. If
     * the file URL has no mounted path in container, then returns null
     *
     * @param url the file URL
     * @return path in the container as a file URL if mounted, else null
     */
    public String getPathInContainer(String url) {
        // sanity check
        if (url == null || !url.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            return null;
        }

        String sourcePath = new PegasusURL(url).getPath();
        StringBuilder replacedURL = new StringBuilder();
        for (Container.MountPoint mp : this.getMountPoints()) {
            String hostSourceDir = mp.getSourceDirectory();
            if (sourcePath.startsWith(hostSourceDir)) {
                // replace the source mount point part of source mp
                // with the destination mp
                sourcePath = sourcePath.replaceFirst(hostSourceDir, mp.getDestinationDirectory());
                // construct the source file url back
                replacedURL.append(PegasusURL.FILE_URL_SCHEME).append("//").append(sourcePath);
                break;
            }
        }

        return replacedURL.length() == 0 ? null : replacedURL.toString();
    }

    /**
     * Returns iterator to the mount points
     *
     * @return
     */
    public Collection<MountPoint> getMountPoints() {
        return this.mMountPoints;
    }

    /**
     * Set the type of the container.
     *
     * @param type
     */
    public void setType(TYPE type) {
        this.mType = type;
    }

    /**
     * Return the type of the container.
     *
     * @return type
     */
    public TYPE getType() {
        return this.mType;
    }

    /**
     * Returns a boolean indicating whether there is a checksum associated with the executable or
     * not.
     *
     * @return
     */
    public boolean hasCheckSum() {
        boolean has = false;
        Profiles p = this.getAllProfiles();
        Metadata m = (Metadata) p.get(Profiles.NAMESPACES.metadata);
        if (m != null) {
            has = m.containsKey(Metadata.CHECKSUM_VALUE_KEY);
        }
        return has;
    }

    /** Sets the bypass flag denoting the file should be bypassed */
    public void setForBypassStaging() {
        mBypassStaging = true;
    }

    /**
     * Sets the bypass flag denoting the file should be bypassed
     *
     * @param value the boolean value to which the flag should be set to.
     */
    public void setForBypassStaging(boolean value) {
        mBypassStaging = value;
    }

    /**
     * Returns whether file should be attempted for bypassing of input file staging.
     *
     * @return true denoting the file can be cleaned up.
     */
    public boolean bypassStaging() {
        return mBypassStaging;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        Container obj;
        try {
            obj = (Container) super.clone();
            obj.setType(mType);
            obj.setImageSite(mImageSite);
            obj.setLFN(this.mLFN);
            obj.setName(this.mName);
            obj.setForBypassStaging(this.mBypassStaging);

            PegasusURL url = this.getImageDefinitionURL();
            if (url != null) {
                obj.setImageDefinitionURL(url.getURL());
            }
            url = this.getImageURL();
            if (url != null) {
                obj.setImageURL(url.getURL());
            }
            // FIX me check for profiles clone
            obj.mProfiles = new Profiles();
            obj.addProfiles(this.mProfiles);

            obj.mMountPoints = new HashSet();
            for (MountPoint m : this.mMountPoints) {
                obj.addMountPoint((MountPoint) m.clone());
            }

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /**
     * Matches two Container objects
     *
     * @return true if the string representation matches else false
     */
    @Override
    public boolean equals(Object obj) {
        // null check
        if (obj == null) {
            return false;
        }

        // see if type of objects match
        if (!(obj instanceof Container)) {
            return false;
        }

        Container c = (Container) obj;
        return this.toString().equals(c.toString());
    }
    /**
     * Returns textual description of object
     *
     * @return
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("cont ").append(this.getLFN()).append("{").append("\n");
        sb.append("\t").append("type     ").append("\t").append(this.getType()).append("\n");
        if (this.getImageURL() != null) {
            sb.append("\t")
                    .append("image    ")
                    .append("\t")
                    .append(this.getImageURL().getURL())
                    .append("\n");
        }
        sb.append("\t").append("image_site ").append("\t").append(this.getImageSite()).append("\n");
        if (this.getImageDefinitionURL() != null) {
            sb.append("\t")
                    .append("dockerfile ")
                    .append("\t")
                    .append(this.getImageDefinitionURL().getURL())
                    .append("\n");
        }
        sb.append("\t").append("bypass ").append("\t").append(this.bypassStaging()).append("\n");
        for (Profile p : this.getProfiles()) {
            sb.append("\t")
                    .append("profile   ")
                    .append("\t")
                    .append(p.getProfileNamespace())
                    .append("\t")
                    .append(p.getProfileKey())
                    .append(" ")
                    .append(p.getProfileValue())
                    .append("\n");
        }
        for (MountPoint mp : this.getMountPoints()) {
            sb.append("\t").append("mount   ").append("\t").append(mp).append("\n");
        }
        sb.append("}").append("\n");
        return sb.toString();
    }

    /** Class to capture the mount point */
    public static class MountPoint implements Cloneable {

        /**
         * Stores the regular expressions necessary to parse a mount point into 3 components source
         * dir, dest dir and options. allows for shell variable names in source and destination
         * directory
         */
        private static final String mRegexExpression =
                "([\\w-/\\.]+(?:\\$\\{[\\w-]+\\})*):([\\w-/\\.]+(?:\\$\\{[\\w-]+\\})*)(:([\\S]+))*";

        /** Stores compiled patterns at first use, quasi-Singleton. */
        private static Pattern mPattern = null;

        /** The source directory on the host machine */
        private String mSourceDirectory;

        /** The destination directory */
        private String mDestDirectory;

        /** The permissions associated */
        private String mMountOption;

        /** The default constructor */
        public MountPoint() {
            if (mPattern == null) {
                mPattern = Pattern.compile(mRegexExpression);
            }
            mSourceDirectory = null;
            mDestDirectory = null;
            mMountOption = null;
        }

        /**
         * The constructor
         *
         * @param mount point string as src-mp:dest-mp:options
         */
        public MountPoint(String mount) {
            if (mPattern == null) {
                mPattern = Pattern.compile(mRegexExpression);
            }
            this.parse(mount);
        }

        /**
         * Parses the mount and populates the internal member variables that can be accessed via the
         * appropriate accessor methods
         *
         * @param mount point string as src-mp:dest-mp:options
         */
        public final void parse(String mount) {
            Matcher m = mPattern.matcher(mount);
            if (m.matches()) {
                this.setSourceDirectory(m.group(1));
                this.setDestinationDirectory(m.group(2));
                if (m.groupCount() == 4) {
                    this.setMountOptions(m.group(4));
                }
            } else {
                throw new RuntimeException("Unable to parse mount " + mount);
            }
        }

        /**
         * Set the source directory
         *
         * @param dir the source directory
         */
        public void setSourceDirectory(String dir) {
            mSourceDirectory = dir;
        }

        /**
         * Returns the source directory
         *
         * @return the source directory
         */
        public String getSourceDirectory() {
            return mSourceDirectory;
        }

        /**
         * Set the destination directory
         *
         * @param dir the destination directory
         */
        public void setDestinationDirectory(String dir) {
            mDestDirectory = dir;
        }

        /**
         * Returns the destination directory
         *
         * @return the destination directory
         */
        public String getDestinationDirectory() {
            return mDestDirectory;
        }

        /**
         * Returns the options associated with mounting
         *
         * @param mount mount option
         */
        public void setMountOptions(String mount) {
            mMountOption = mount;
        }

        /** Returns the options associated with mounting */
        public String getMountOptions() {
            return mMountOption;
        }

        /**
         * Return the string description
         *
         * @return
         */
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(this.getSourceDirectory()).append(":").append(this.getDestinationDirectory());
            if (this.getMountOptions() != null) {
                sb.append(":").append(this.getMountOptions());
            }

            return sb.toString();
        }

        /**
         * Matches two Mount Point objects. The primary key in this case is the source directory and
         * the destination directory
         *
         * @return true if directories combo match
         */
        public boolean equals(Object obj) {
            // null check
            if (obj == null) {
                return false;
            }

            // see if type of objects match
            if (!(obj instanceof MountPoint)) {
                return false;
            }

            MountPoint mp = (MountPoint) obj;
            String dir1 = this.getSourceDirectory();
            String dir2 = mp.getSourceDirectory();

            // mp with null dirs are assumed to match
            boolean result =
                    (dir1 == null && dir2 == null)
                            || (dir1 != null && dir2 != null && dir1.equals(dir2));

            if (result) {
                dir1 = this.getDestinationDirectory();
                dir2 = mp.getDestinationDirectory();

                // mp with null dirs are assumed to match
                result =
                        (dir1 == null && dir2 == null)
                                || (dir1 != null && dir2 != null && dir1.equals(dir2));
            }
            return result;
        }

        /**
         * Calculate a hash code value for the object to support hash tables.
         *
         * @return a hash code value for the object.
         */
        public int hashCode() {
            return this.toString().hashCode();
        }

        /**
         * Returns the clone of the object.
         *
         * @return the clone
         */
        public Object clone() {
            MountPoint obj;
            try {
                obj = (MountPoint) super.clone();
                obj.setSourceDirectory(this.getSourceDirectory());
                obj.setDestinationDirectory(this.getDestinationDirectory());
                obj.setMountOptions(this.getMountOptions());

            } catch (CloneNotSupportedException e) {
                // somewhere in the hierarchy chain clone is not implemented
                throw new RuntimeException(
                        "Clone not implemented in the base class of " + this.getClass().getName(),
                        e);
            }
            return obj;
        }
    }

    /**
     * Custom deserializer for YAML representation of Container
     *
     * @author Karan Vahi
     */
    static class JsonDeserializer extends CatalogEntryJsonDeserializer<Container> {

        public JsonDeserializer() {}

        /**
         * Deserializes a Transformation YAML description of the type
         *
         * <pre>
         * - name: centos-pegasus
         *   type: docker
         *   image: docker:///rynge/montage:latest
         *   mounts:
         *      - /Volumes/Work/lfs1:/shared-data/:ro
         *      - /Volumes/Work/lfs2:/shared-data2/:ro
         *
         *   profiles:
         *     env:
         *       JAVA_HOME: /opt/java/1.6
         * </pre>
         *
         * @param parser
         * @param dc
         * @return
         * @throws IOException
         * @throws JsonProcessingException
         */
        @Override
        public Container deserialize(JsonParser parser, DeserializationContext dc)
                throws IOException, JsonProcessingException {
            ObjectCodec oc = parser.getCodec();
            JsonNode node = oc.readTree(parser);
            Container container = new Container();
            Metadata checksum = null;
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                TransformationCatalogKeywords reservedKey =
                        TransformationCatalogKeywords.getReservedKey(key);
                if (reservedKey == null) {
                    this.complainForIllegalKey(
                            TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName(),
                            key,
                            node);
                }

                switch (reservedKey) {
                    case NAME:
                        String containerName = node.get(key).asText();
                        container.setName(containerName);
                        break;

                    case TYPE:
                        String type = node.get(key).asText();
                        container.setType(Container.TYPE.valueOf(type));
                        break;

                    case CONTAINER_IMAGE:
                        String url = node.get(key).asText();
                        container.setImageURL(url);
                        break;

                    case CONTAINER_IMAGE_SITE:
                        String imageSite = node.get(key).asText();
                        container.setImageSite(imageSite);
                        break;

                    case BYPASS:
                        container.setForBypassStaging(node.get(key).asBoolean());
                        break;

                    case CONTAINER_DOCKERFILE:
                        String dockerFile = node.get(key).asText();
                        container.setImageDefinitionURL(dockerFile);
                        break;

                    case CONTAINER_MOUNT:
                        List<String> mps =
                                this.createMountPoints(
                                        node.get(
                                                TransformationCatalogKeywords.CONTAINER_MOUNT
                                                        .getReservedName()));
                        for (String mountPoint : mps) {
                            container.addMountPoint(mountPoint);
                        }
                        break;

                    case METADATA:
                        container.addProfiles(this.createMetadata(node.get(key)));
                        break;

                    case CHECKSUM:
                        checksum =
                                this.createChecksum(
                                        node.get(key),
                                        TransformationCatalogKeywords.TRANSFORMATIONS
                                                .getReservedName());
                        break;

                    case PROFILES:
                        JsonNode profilesNode = node.get(key);
                        if (profilesNode != null) {
                            parser = profilesNode.traverse(oc);
                            Profiles profiles = parser.readValueAs(Profiles.class);
                            container.addProfiles(profiles);
                        }
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                TransformationCatalogKeywords.CONTAINERS.getReservedName(),
                                key,
                                node);
                }
            }
            if (checksum != null) {
                // PM-1420 merge metadata profiles to include checksum
                Metadata m =
                        (Metadata) container.getAllProfiles().get(Profiles.NAMESPACES.metadata);
                if (m == null) {
                    // no metadata in the base, add checksum information into the base
                    container.getAllProfiles().set(m);
                } else {
                    // merge with existing metadata
                    m.merge(checksum);
                }
            }

            return container;
        }

        /**
         * Creates a list of mount points for the container
         *
         * <pre>
         *    - /Volumes/Work/lfs1:/shared-data/:ro
         *    - /Volumes/Work/lfs2:/shared-data2/:ro
         * </pre>
         *
         * @param node
         * @return
         */
        protected List<String> createMountPoints(JsonNode node) {
            List<String> mps = new LinkedList();
            if (node.isArray()) {
                for (JsonNode mpNode : node) {
                    mps.add(mpNode.asText());
                }
            } else {
                throw new ScannerException("containers.mount: value should be of type array ");
            }
            return mps;
        }
    }

    /**
     * Custom serializer for YAML representation of Container
     *
     * @author Karan Vahi
     */
    static class JsonSerializer extends PegasusJsonSerializer<Container> {

        public JsonSerializer() {}

        /**
         * Serializes contents into YAML representation
         *
         * @param container
         * @param gen
         * @param sp
         * @throws IOException
         */
        public void serialize(Container container, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();
            writeStringField(
                    gen, TransformationCatalogKeywords.NAME.getReservedName(), container.getName());
            writeStringField(
                    gen,
                    TransformationCatalogKeywords.TYPE.getReservedName(),
                    container.getType().toString());

            if (container.getImageURL() != null) {
                writeStringField(
                        gen,
                        TransformationCatalogKeywords.CONTAINER_IMAGE.getReservedName(),
                        container.getImageURL().getURL());
            }
            writeStringField(
                    gen,
                    TransformationCatalogKeywords.CONTAINER_IMAGE_SITE.getReservedName(),
                    container.getImageSite());

            gen.writeBooleanField(
                    TransformationCatalogKeywords.BYPASS.getReservedName(),
                    container.bypassStaging());

            if (!container.getMountPoints().isEmpty()) {
                gen.writeArrayFieldStart(
                        TransformationCatalogKeywords.CONTAINER_MOUNT.getReservedName());
                for (MountPoint mp : container.getMountPoints()) {
                    gen.writeObject(mp.toString());
                }
                gen.writeEndArray();
            }

            Profiles profiles = container.getAllProfiles();
            if (profiles != null) {
                this.serializeProfiles(gen, profiles);
            }

            gen.writeEndObject();
        }

        /**
         * Special handling for profiles serialization to ensure that metadata and checksum
         * information is serialized at the same level as profiles and not nested inside .
         *
         * @param gen
         * @param profiles
         * @throws IOException
         */
        private void serializeProfiles(JsonGenerator gen, Profiles profiles) throws IOException {
            // PM-1617 metadata and checksum are at the profiles level , not nested in there
            // remove the metadata and add back after serializing profiles
            Metadata m = (Metadata) profiles.remove(Profiles.NAMESPACES.metadata);

            if (!profiles.isEmpty()) {
                gen.writeFieldName(TransformationCatalogKeywords.PROFILES.getReservedName());
                gen.writeObject(profiles);
            }
            gen.writeObject(m);
            profiles.set(m);
        }
    }
}
