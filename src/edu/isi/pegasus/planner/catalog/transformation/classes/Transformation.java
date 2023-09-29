/**
 * Copyright 2007-2020 University Of Southern California
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.TextNode;
import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.impl.Abstract;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A convenience class for yaml serialization / deserialization of transformation catalog. Should
 * not be used for anything else
 *
 * @author Karan Vahi
 */
@JsonDeserialize(using = TransformationDeserializer.class)
public class Transformation {

    private TransformationCatalogEntry mBaseEntry;

    private List<TransformationCatalogEntry> mSiteEntries;

    public Transformation() {
        mSiteEntries = new LinkedList();
    }

    public void setBaseTCEntry(TransformationCatalogEntry entry) {
        mBaseEntry = entry;
    }

    public void addSiteTCEntry(TransformationCatalogEntry entry) {
        this.mSiteEntries.add(entry);
    }

    public List<TransformationCatalogEntry> getTransformationCatalogEntries() {
        List<TransformationCatalogEntry> entries = new LinkedList();
        if (this.mSiteEntries.isEmpty()) {
            entries.add(mBaseEntry);
            return entries;
        }

        for (TransformationCatalogEntry siteEntry : mSiteEntries) {
            entries.add(this.addSiteInformation(mBaseEntry, siteEntry));
        }
        return entries;
    }

    /**
     * Adds site specific information from to the base tx, and returns a new merged tx
     *
     * @param base
     * @param from
     * @return
     */
    protected TransformationCatalogEntry addSiteInformation(
            TransformationCatalogEntry base, TransformationCatalogEntry from) {
        TransformationCatalogEntry entry = (TransformationCatalogEntry) base.clone();
        SysInfo sysInfo = new SysInfo();

        entry.setResourceId(from.getResourceId());
        entry.setSysInfo(from.getSysInfo());
        entry.setType(from.getType());
        entry.addProfiles(from.getProfiles());
        entry.setPhysicalTransformation(from.getPhysicalTransformation());
        entry.setForBypassStaging(from.bypassStaging());
        entry.setContainer(from.getContainer());
        return entry;
    }
}

/**
 * Custom deserializer for YAML representation of Transformation
 *
 * @author Karan Vahi
 */
class TransformationDeserializer extends CatalogEntryJsonDeserializer<Transformation> {

    /**
     * Deserializes a Transformation YAML description of the type
     *
     * <pre>
     * namespace: "example"
     * name: "keg"
     * version: "1.0"
     * profiles:
     *   env:
     *     APP_HOME: "/tmp/myscratch"
     *     JAVA_HOME: "/opt/java/1.6"
     *   pegasus:
     *     clusters.num: "1"
     * checksum:
     *      sha256: abc123
     *   metadata:
     *     owner: vahi
     *     size: 1024
     * requires:
     *   - anotherTr
     * sites:
     *   name: "isi"
     *   type: "installed"
     *   pfn: "/path/to/keg"
     *   arch: "x86_64"
     *   os.type: "linux"
     *   os.release: "fc"
     *   os.version: "1.0"
     *   profiles:
     *     env:
     *       Hello: World
     *       JAVA_HOME: /bin/java.1.6
     *     condor:
     *       FOO: bar
     *   container: centos-pegasus
     * </pre>
     *
     * @param parser
     * @param dc
     * @return
     * @throws IOException
     * @throws JsonProcessingException
     */
    @Override
    public Transformation deserialize(JsonParser parser, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec oc = parser.getCodec();
        JsonNode node = oc.readTree(parser);
        Transformation tx = new Transformation();
        TransformationCatalogEntry base = new TransformationCatalogEntry();
        Metadata checksum = null;
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            TransformationCatalogKeywords reservedKey =
                    TransformationCatalogKeywords.getReservedKey(key);
            if (reservedKey == null) {
                this.complainForIllegalKey(
                        TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName(), key, node);
            }

            switch (reservedKey) {
                case NAMESPACE:
                    base.setLogicalNamespace(node.get(key).asText());
                    break;

                case NAME:
                    base.setLogicalName(node.get(key).asText());
                    break;

                case VERSION:
                    base.setLogicalVersion(node.get(key).asText());
                    break;

                case METADATA:
                    base.addProfiles(this.createMetadata(node.get(key)));
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
                        base.addProfiles(profiles);
                    }
                    break;

                case HOOKS:
                    JsonNode hooksNode = node.get(key);
                    if (hooksNode != null) {
                        parser = hooksNode.traverse(oc);
                        Notifications n = parser.readValueAs(Notifications.class);
                        base.addNotifications(n);
                    }
                    break;

                case REQUIRES:
                    JsonNode requiresNode =
                            node.get(TransformationCatalogKeywords.REQUIRES.getReservedName());
                    if (requiresNode.isArray()) {
                        for (JsonNode dependentNode : requiresNode) {
                            base.addDependantTransformation(dependentNode.asText());
                        }
                    } else {
                        throw new CatalogException("requires: value should be of type array ");
                    }
                    break;

                case SITES:
                    JsonNode sitesNode = node.get(key);
                    if (sitesNode.isArray()) {
                        for (JsonNode siteNode : sitesNode) {
                            TransformationCatalogEntry entry =
                                    getSiteSpecificEntry(parser, siteNode, base);
                            tx.addSiteTCEntry(entry);
                        }
                    } else {
                        throw new CatalogException("sites: value should be of type array ");
                    }
                    break;

                default:
                    this.complainForUnsupportedKey(
                            TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName(),
                            key,
                            node);
            }
        }

        if (checksum != null) {
            // PM-1617 merge metadata profiles to include checksum
            Metadata m = (Metadata) base.getProfilesNamepsace(Profiles.NAMESPACES.metadata);
            if (m == null) {
                // no metadata in the base, add checksum information into the base
                for (Iterator<String> it = checksum.getProfileKeyIterator(); it.hasNext(); ) {
                    String key = it.next();
                    base.addProfile(
                            new Profile(checksum.namespaceName(), key, (String) checksum.get(key)));
                }
            } else {
                // merge with existing metadata
                m.merge(checksum);
            }
        }
        tx.setBaseTCEntry(base);
        return tx;
    }

    /**
     * Parses site information from JsonNode and adds it to the transformation catalog entry.
     *
     * <pre>
     * name: "isi"
     * type: "installed"
     * pfn: "/path/to/keg"
     * bypass: true
     * arch: "x86_64"
     * os.type: "linux"
     * os.release: "fc"
     * os.version: "1.0"
     * profiles:
     *   env:
     *       Hello: World
     *       JAVA_HOME: /bin/java.1.6
     *   condor:
     *       FOO: bar
     * container: centos-pegasus
     * </pre>
     *
     * @param parser
     * @param node
     * @param base
     */
    protected TransformationCatalogEntry getSiteSpecificEntry(
            JsonParser parser, JsonNode node, TransformationCatalogEntry base) throws IOException {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        SysInfo sysInfo = new SysInfo();
        ObjectCodec oc = parser.getCodec();

        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            TransformationCatalogKeywords reservedKey =
                    TransformationCatalogKeywords.getReservedKey(key);

            switch (reservedKey) {
                case NAME:
                    String siteName = node.get(key).asText();
                    entry.setResourceId(siteName);
                    break;

                case BYPASS:
                    entry.setForBypassStaging(node.get(key).asBoolean());
                    break;

                case SITE_ARCHITECTURE:
                    String architecture = node.get(key).asText();
                    sysInfo.setArchitecture(SysInfo.Architecture.valueOf(architecture));
                    break;

                case SITE_OS_TYPE:
                    String os = node.get(key).asText();
                    sysInfo.setOS(SysInfo.OS.valueOf(os));
                    break;

                case SITE_OS_RELEASE:
                    String release = node.get(key).asText();
                    sysInfo.setOSRelease(release);
                    break;

                case SITE_OS_VERSION:
                    String osVersion = node.get(key).asText();
                    sysInfo.setOSVersion(String.valueOf(osVersion));
                    break;

                case TYPE:
                    String type = node.get(key).asText();
                    entry.setType(TCType.valueOf(type.toUpperCase()));
                    break;

                case PROFILES:
                    JsonNode profilesNode = node.get(key);
                    if (profilesNode != null) {
                        parser = profilesNode.traverse(oc);
                        Profiles profiles = parser.readValueAs(Profiles.class);
                        entry.addProfiles(profiles);
                    }
                    break;

                case METADATA:
                    entry.addProfiles(this.createMetadata(node.get(key)));
                    break;

                case SITE_PFN:
                    String pfn = node.get(key).asText();
                    entry.setPhysicalTransformation(pfn);
                    break;

                case SITE_CONTAINER_NAME:
                    JsonNode containerNode = node.get(key);
                    if (!(containerNode instanceof TextNode)) {
                        throw new CatalogException(
                                "Container node is fully defined in the tx "
                                        + base.getLogicalTransformation()
                                        + " instead of being a reference "
                                        + node);
                    }
                    String containerName = containerNode.asText();
                    entry.setContainer(new Container(containerName));
                    break;

                default:
                    this.complainForUnsupportedKey(
                            TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName(),
                            key,
                            node);
                    break;
            }
        }
        entry.setSysInfo(sysInfo);

        Abstract.modifyForFileURLS(entry);
        return entry;
    }
}
