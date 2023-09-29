/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.catalog.site.classes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Directory class used for Site Catalog Schema version 4 onwards. The type of directory is
 * determined based on type attribute rather than having separate classes for it.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
@JsonSerialize(using = DirectorySerializer.class)
@JsonDeserialize(using = DirectoryDeserializer.class)
public class Directory extends DirectoryLayout {

    /** Enumerates the new directory types supported in this schema */
    public static enum TYPE {
        shared_scratch("shared-scratch"),
        shared_storage("shared-storage"),
        local_scratch("local-scratch"),
        local_storage("local-storage");

        public static TYPE value(String name) {
            return TYPE.valueOf(name.replaceAll("-", "_"));
        }

        private String mValue;

        /**
         * The constructor
         *
         * @param value the string value to return
         */
        TYPE(String value) {
            mValue = value;
        }

        /**
         * The value associated with the enum
         *
         * @return
         */
        private String getValue() {
            return this.mValue;
        }

        /**
         * Override of the toString method to return
         *
         * @return
         */
        public String toString() {
            return this.getValue();
        }
    }

    /** Enumerates the new directory types supported in this schema */
    public static enum YAML_TYPE {
        sharedScratch,
        sharedStorage,
        localScratch,
        localStorage;
    }

    /**
     * Maps the values for yamlType key in yaml schema to old types
     *
     * @param yamlType
     * @return
     */
    public static TYPE yamlTypeToType(String yamlType) {
        return yamlTypeToType(YAML_TYPE.valueOf(yamlType));
    }

    /**
     * Maps the values for yamlType key in yaml schema to old types
     *
     * @param yamlType
     * @return
     */
    public static TYPE yamlTypeToType(YAML_TYPE yamlType) {
        TYPE type = TYPE.shared_scratch;
        switch (yamlType) {
            case sharedScratch:
                type = TYPE.shared_scratch;
                break;

            case sharedStorage:
                type = TYPE.shared_storage;
                break;

            case localScratch:
                type = TYPE.local_scratch;
                break;

            case localStorage:
                type = TYPE.local_storage;
                break;

            default:
                throw new SiteCatalogException("Unkown type value " + yamlType);
        }

        return type;
    }

    /**
     * Maps the values for old types to camel case types in the YAML schema
     *
     * @param yamlType
     * @return
     */
    public static YAML_TYPE typeToYAMLType(String type) {
        return typeToYAMLType(TYPE.value(type));
    }

    /**
     * Maps the values for old types to camel case types in the YAML schema
     *
     * @param yamlType
     * @return
     */
    public static YAML_TYPE typeToYAMLType(TYPE type) {
        YAML_TYPE yamlType = YAML_TYPE.sharedScratch;
        switch (type) {
            case shared_scratch:
                yamlType = YAML_TYPE.sharedScratch;
                break;

            case shared_storage:
                yamlType = YAML_TYPE.sharedStorage;
                break;

            case local_scratch:
                yamlType = YAML_TYPE.localScratch;
                break;

            case local_storage:
                yamlType = YAML_TYPE.localStorage;
                break;

            default:
                throw new SiteCatalogException("Unkown type value " + yamlType);
        }

        return yamlType;
    }

    /** The yamlType of directory */
    private TYPE mType;

    /**
     * a boolean indicating whether there is a shared filsystem access from the worker nodes in the
     * site to this directory.
     */
    private boolean mHasSharedFileSystem;

    /** Default constructor */
    public Directory() {
        super();
        mHasSharedFileSystem = false;
    }

    /**
     * Convenience constructor for adapter class
     *
     * @param directory the directory layout object
     * @param type the yamlType associated
     */
    public Directory(DirectoryLayout directory, TYPE type) {
        super(directory);
        this.setType(type);
        mHasSharedFileSystem = false;
    }

    /**
     * Returns a boolean indicating whether there is a shared filsystem access from the worker nodes
     * in the site to this directory.
     *
     * @param value boolean value
     */
    public void setSharedFileSystemAccess(boolean value) {
        this.mHasSharedFileSystem = value;
    }

    /**
     * Returns a boolean indicating whether there is a shared filsystem access from the worker nodes
     * in the site to this directory.
     *
     * @return
     */
    public boolean hasSharedFileSystemAccess() {
        return this.mHasSharedFileSystem;
    }

    /**
     * Accept method for the SiteData classes that accepts a visitor
     *
     * @param visitor the visitor to be used
     * @exception IOException if something fishy happens to the stream.
     */
    public void accept(SiteDataVisitor visitor) throws IOException {
        visitor.visit(this);

        // traverse through all the file servers
        // for( FileServer server : this.mFileServers ){
        //    server.accept(visitor);
        // }

        for (OPERATION op : FileServer.OPERATION.values()) {
            List<FileServer> servers = this.mFileServers.get(op);
            for (FileServer server : servers) {
                server.accept(visitor);
            }
        }

        // profiles are handled in the depart method
        visitor.depart(this);
    }

    /**
     * Set the yamlType of directory
     *
     * @param type the yamlType of directory
     */
    public void setType(String type) {
        mType = TYPE.value(type);
    }

    /**
     * Set the yamlType of directory
     *
     * @param type the yamlType of directory
     */
    public void setType(Directory.TYPE type) {
        mType = type;
    }

    /**
     * Set the yamlType of directory
     *
     * @return the yamlType of directory
     */
    public TYPE getType() {
        return mType;
    }

    /**
     * @param writer
     * @param indent
     * @throws IOException
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");
        String newIndent = indent + "\t";

        // sanity check?
        if (this.isEmpty()) {
            return;
        }

        // write out the  xml element
        writer.write(indent);
        writer.write("<directory ");
        writeAttribute(writer, "type", this.getType().toString());
        writer.write(">");
        writer.write(newLine);

        // iterate through all the file servers
        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
            for (Iterator<FileServer> it = this.getFileServersIterator(op); it.hasNext(); ) {
                FileServer fs = it.next();
                fs.toXML(writer, newIndent);
            }
        }

        // write out the internal mount point
        this.getInternalMountPoint().toXML(writer, newIndent);

        writer.write(indent);
        writer.write("</directory>");
        writer.write(newLine);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        Directory obj;
        obj = (Directory) super.clone();

        obj.setType(this.getType());

        return obj;
    }

    /**
     * Matches two Directory objects
     *
     * @return true if directories match
     */
    @Override
    public boolean equals(Object obj) {
        // null check
        if (obj == null) {
            return false;
        }

        // see if type of objects match
        if (!(obj instanceof Directory)) {
            return false;
        }
        Directory dir = (Directory) obj;

        // short cut
        return this.toString().equals(dir.toString());
    }

    public static void main(String[] args) {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        /*SimpleModule module = new SimpleModule();
        module.addDeserializer(FileServer.class, new FileServerDeserializer());
        mapper.registerModule(module);
        */
        String test =
                "  type: sharedScratch\n"
                        + "  path: /tmp/workflows/scratch\n"
                        + "  freeSize: 1GB\n"
                        + "  totalSize: 122GB\n"
                        + "  fileServers:\n"
                        + "    - operation: all\n"
                        + "      url: file:///tmp/workflows/scratch\n";
        Directory dir = null;
        try {
            dir = mapper.readValue(test, Directory.class);
            System.out.println(dir);
            System.out.println(mapper.writeValueAsString(dir));
        } catch (IOException ex) {
            Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

/**
 * Custom deserializer for YAML representation of Directory
 *
 * @author Karan Vahi
 */
class DirectoryDeserializer extends SiteDataJsonDeserializer<Directory> {

    /**
     * Deserializes a Directory YAML description of the type
     *
     * <pre>
     * yamlType: sharedScratch
     * path: /tmp/workflows/scratch
     * fileServers:
     * - operation: all
     * url: file:///tmp/workflows/scratch
     * </pre>
     *
     * @param parser
     * @param dc
     * @return
     * @throws IOException
     * @throws JsonProcessingException
     */
    @Override
    public Directory deserialize(JsonParser parser, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec oc = parser.getCodec();
        JsonNode node = oc.readTree(parser);
        Directory directory = new Directory();

        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            SiteCatalogKeywords reservedKey = SiteCatalogKeywords.getReservedKey(key);
            if (reservedKey == null) {
                this.complainForIllegalKey(
                        SiteCatalogKeywords.DIRECTORIES.getReservedName(), key, node);
            }

            switch (reservedKey) {
                case TYPE:
                    directory.setType(Directory.yamlTypeToType(node.get(key).asText()));
                    break;

                case PATH:
                    directory.getInternalMountPoint().setMountPoint(node.get(key).asText());
                    break;

                case SHARED_FILESYSTEM:
                    directory.setSharedFileSystemAccess(node.get(key).asBoolean());
                    break;

                case FILESERVERS:
                    JsonNode fileServersNodes = node.get(key);
                    if (fileServersNodes != null) {
                        if (fileServersNodes.isArray()) {
                            for (JsonNode fileServerNode : fileServersNodes) {
                                parser = fileServerNode.traverse(oc);
                                FileServer fs = parser.readValueAs(FileServer.class);
                                directory.addFileServer(fs);
                            }
                        }
                    }
                    break;

                    // defined in schema but we don't do anything about it
                case FREE_SIZE:
                    directory.getInternalMountPoint().setFreeSize(node.get(key).asText());
                    break;

                case TOTAL_SIZE:
                    directory.getInternalMountPoint().setTotalSize(node.get(key).asText());
                    break;

                default:
                    this.complainForUnsupportedKey(
                            SiteCatalogKeywords.DIRECTORIES.getReservedName(), key, node);
            }
        }

        return directory;
    }
}
/**
 * Custom serializer for YAML representation of Directory
 *
 * @author Karan Vahi
 */
class DirectorySerializer extends PegasusJsonSerializer<Directory> {

    public DirectorySerializer() {}

    /**
     * Serializes contents into YAML representation
     *
     * @param directory
     * @param gen
     * @param sp
     * @throws IOException
     */
    public void serialize(Directory directory, JsonGenerator gen, SerializerProvider sp)
            throws IOException {
        InternalMountPoint imp = directory.getInternalMountPoint();
        gen.writeStartObject();
        writeStringField(
                gen,
                SiteCatalogKeywords.TYPE.getReservedName(),
                Directory.typeToYAMLType(directory.getType().toString()).toString());
        writeStringField(gen, SiteCatalogKeywords.PATH.getReservedName(), imp.getMountPoint());
        writeStringField(gen, SiteCatalogKeywords.FREE_SIZE.getReservedName(), imp.getFreeSize());
        writeStringField(gen, SiteCatalogKeywords.TOTAL_SIZE.getReservedName(), imp.getTotalSize());
        gen.writeBooleanField(
                SiteCatalogKeywords.SHARED_FILESYSTEM.getReservedName(),
                directory.hasSharedFileSystemAccess());

        /*gen.writeArrayFieldStart(SiteCatalogKeywords.FILESERVERS.getReservedName());
        // iterate through all the file servers
        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
            for (Iterator<FileServer> it = directory.getFileServersIterator(op); it.hasNext();) {
                FileServer fs = it.next();
                gen.writeObject(fs);
            }
        }
        gen.writeEndArray();
        */
        List<FileServer> fservers = new LinkedList();
        // iterate through all the file servers
        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
            for (Iterator<FileServer> it = directory.getFileServersIterator(op); it.hasNext(); ) {
                FileServer fs = it.next();
                fservers.add(fs);
            }
        }
        writeArray(gen, SiteCatalogKeywords.FILESERVERS.getReservedName(), fservers);

        gen.writeEndObject();
    }
}
