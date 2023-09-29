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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.XMLWriter;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.Notifications;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * The Transformation Catalog object the represent the entries in the DAX transformation section.
 *
 * @author gmehta
 * @version $Revision$
 */
public class Executable extends CatalogType {

    /** ARCH Types */
    public static enum ARCH {
        X86,
        x86,
        X86_64,
        x86_64,
        PPC,
        ppc,
        PPC_64,
        ppc_64,
        IA64,
        ia64,
        SPARCV7,
        sparcv7,
        SPARCV9,
        sparcv9,
        ppc64le,
        aarch64
    }

    /** OS Types */
    public static enum OS {
        LINUX,
        linux,
        SUNOS,
        sunos,
        AIX,
        aix,
        MACOSX,
        macosx,
        WINDOWS,
        windows
    }
    /** Namespace of the executable */
    protected String mNamespace;
    /** Name of the executable */
    protected String mName;
    /** Version of the executable */
    protected String mVersion;
    /** Architecture the executable is compiled for */
    protected ARCH mArch;
    /** Os the executable is compiled for */
    protected OS mOs;
    /** Os release the executable is compiled for */
    protected String mOsRelease;
    /** OS version the executable is compiled for */
    protected String mOsVersion;
    /** Glibc the executable is compiled for */
    protected String mGlibc;
    /** Flag to mark if the executable is installed or can be staged. */
    protected boolean mInstalled = true;

    /** List of Notification objects */
    protected List<Invoke> mInvokes;

    /** Other executables this executable requires */
    protected Set<Executable> mRequires;

    /**
     * Create a new executable
     *
     * @param name name
     */
    public Executable(String name) {
        this("", name, "");
    }

    /**
     * Copy Constructor
     *
     * @param e executable to copy from
     */
    public Executable(Executable e) {
        super(e);
        this.mNamespace = e.mNamespace;
        this.mName = e.mName;
        this.mVersion = e.mVersion;
        this.mArch = e.mArch;
        this.mOs = e.mOs;
        this.mOsRelease = e.mOsRelease;
        this.mOsVersion = e.mOsVersion;
        this.mGlibc = e.mGlibc;
        this.mInstalled = e.mInstalled;
        this.mInvokes = new LinkedList<Invoke>(e.mInvokes);
    }

    /**
     * Create a new Executable
     *
     * @param namespace the namespace
     * @param name the name
     * @param version the version
     */
    public Executable(String namespace, String name, String version) {
        super();
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;
        mVersion = (version == null) ? "" : version;
        mInvokes = new LinkedList<Invoke>();
        mRequires = new HashSet<Executable>();
    }

    /**
     * Get the name of the executable
     *
     * @return String
     */
    public String getName() {
        return mName;
    }

    /**
     * Get the namespace of the executable
     *
     * @return namespace
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Get the version of the executable
     *
     * @return version
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Return the list of Notification objects
     *
     * @return List of Invoke objects
     */
    public List<Invoke> getInvoke() {
        return mInvokes;
    }

    /**
     * Return the list of Notification objects (same as getInvoke)
     *
     * @return List of Invoke objects
     */
    public List<Invoke> getNotification() {
        return getInvoke();
    }

    /**
     * Add a Notification for this Executable same as addNotification
     *
     * @param when when to invoke
     * @param what what executable to invoke including the arguments
     * @return Executable
     */
    public Executable addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    /**
     * Add a Notification for this Executable same as addInvoke
     *
     * @param when when to invoke
     * @param what what executable to invoke including the arguments
     * @return Executable
     */
    public Executable addNotification(Invoke.WHEN when, String what) {
        return addInvoke(when, what);
    }

    /**
     * Add a Notification for this Executable Same as add Notification
     *
     * @param invoke the invoke object containing the notification
     * @return Executable
     */
    public Executable addInvoke(Invoke invoke) {
        mInvokes.add(invoke.clone());
        return this;
    }

    /**
     * Add a Notification for this Executable Same as addInvoke
     *
     * @param invoke the invoke object containing the notification
     * @return Executable
     */
    public Executable addNotification(Invoke invoke) {
        return addInvoke(invoke);
    }

    /**
     * Add a List of Notifications for this Executable Same as addNotifications
     *
     * @param invokes list of notification objects
     * @return Executable
     */
    public Executable addInvokes(List<Invoke> invokes) {
        for (Invoke invoke : invokes) {
            this.addInvoke(invoke);
        }
        return this;
    }

    /**
     * Add a List of Notifications for this Executable. Same as addInvokes
     *
     * @param invokes list of notification objects
     * @return Executable
     */
    public Executable addNotifications(List<Invoke> invokes) {
        return addInvokes(invokes);
    }

    /**
     * Set the architecture the executable is compiled for
     *
     * @param arch the architecture
     * @return the Executable object that was modified
     */
    public Executable setArchitecture(ARCH arch) {
        mArch = arch;
        return this;
    }

    /**
     * Set the OS the executable is compiled for
     *
     * @param os the OS
     * @return the Executable object that was modified
     */
    public Executable setOS(OS os) {
        mOs = os;
        return this;
    }

    /**
     * Set the osrelease the executable is compiled for
     *
     * @param osrelease the os release
     * @return the Executable object that was modified
     */
    public Executable setOSRelease(String osrelease) {
        mOsRelease = osrelease;
        return this;
    }

    /**
     * Set the osversion the executable is compiled for
     *
     * @param osversion os version
     * @return the Executable object that was modified
     */
    public Executable setOSVersion(String osversion) {
        mOsVersion = osversion;
        return this;
    }

    /**
     * Set the glibc this executable is compiled for
     *
     * @param glibc glibc version
     * @return the Executable object that was modified
     */
    public Executable setGlibc(String glibc) {
        mGlibc = glibc;
        return this;
    }

    /**
     * set the installed flag on the executable. Default is installed
     *
     * @return the Executable object that was modified
     */
    public Executable setInstalled() {
        mInstalled = true;
        return this;
    }

    /**
     * Unset the installed flag on the executable. Default is installed.
     *
     * @return the Executable object that was modified
     */
    public Executable unsetInstalled() {
        mInstalled = false;
        return this;
    }

    /**
     * Set the installed flag on the executable.Default is installed
     *
     * @param installed the installed flag
     * @return the Executable object that was modified
     */
    public Executable setInstalled(boolean installed) {
        mInstalled = installed;
        return this;
    }

    /**
     * Check if the executable is of type installed.
     *
     * @return Boolean
     */
    public boolean getInstalled() {
        return mInstalled;
    }

    /**
     * Get the architecture the Executable is compiled for
     *
     * @return Architecture
     */
    public ARCH getArchitecture() {
        return mArch;
    }

    /**
     * Get the OS the Executable is compiled for
     *
     * @return the OS
     */
    public OS getOS() {
        return mOs;
    }

    /**
     * Get the OS release set for this executable. Returns empty string if not set
     *
     * @return String
     */
    public String getOsRelease() {
        return (mOsRelease == null) ? "" : mOsRelease;
    }

    /**
     * Get the OS version set for this executable.
     *
     * @return String
     */
    public String getOsVersion() {
        return (mOsVersion == null) ? "" : mOsVersion;
    }

    /**
     * Get the Glibc version if any set for this file. Returns empty string if not set
     *
     * @return String
     */
    public String getGlibc() {
        return (mGlibc == null) ? "" : mGlibc;
    }

    /**
     * Return boolean indicating whether executable or not
     *
     * @return boolean
     */
    public boolean isExecutable() {
        return true;
    }

    /**
     * Get the set of executables that this executable requires
     *
     * @return Set of Executable this main executable requires
     */
    public Set<Executable> getRequirements() {
        return this.mRequires;
    }

    /**
     * Add another executable as a requirement to this executable
     *
     * @param e dependent executable
     * @return instance to the Executable that was modified
     */
    public Executable addRequirement(Executable e) {
        this.mRequires.add(e);

        return this;
    }

    /**
     * Compares whether an object is equal to this instance of Executable or not
     *
     * @param obj object to compare against
     * @return boolean
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Executable other = (Executable) obj;
        if ((this.mNamespace == null)
                ? (other.mNamespace != null)
                : !this.mNamespace.equals(other.mNamespace)) {
            return false;
        }
        if ((this.mName == null) ? (other.mName != null) : !this.mName.equals(other.mName)) {
            return false;
        }
        if ((this.mVersion == null)
                ? (other.mVersion != null)
                : !this.mVersion.equals(other.mVersion)) {
            return false;
        }
        if (this.mArch != other.mArch) {
            return false;
        }
        if (this.mOs != other.mOs) {
            return false;
        }
        if ((this.mOsRelease == null)
                ? (other.mOsRelease != null)
                : !this.mOsRelease.equals(other.mOsRelease)) {
            return false;
        }
        if ((this.mOsVersion == null)
                ? (other.mOsVersion != null)
                : !this.mOsVersion.equals(other.mOsVersion)) {
            return false;
        }
        if ((this.mGlibc == null) ? (other.mGlibc != null) : !this.mGlibc.equals(other.mGlibc)) {
            return false;
        }
        if (this.mInstalled != other.mInstalled) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + (this.mNamespace != null ? this.mNamespace.hashCode() : 0);
        hash = 53 * hash + (this.mName != null ? this.mName.hashCode() : 0);
        hash = 53 * hash + (this.mVersion != null ? this.mVersion.hashCode() : 0);
        hash = 53 * hash + (this.mArch != null ? this.mArch.hashCode() : 0);
        hash = 53 * hash + (this.mOs != null ? this.mOs.hashCode() : 0);
        hash = 53 * hash + (this.mOsRelease != null ? this.mOsRelease.hashCode() : 0);
        hash = 53 * hash + (this.mOsVersion != null ? this.mOsVersion.hashCode() : 0);
        hash = 53 * hash + (this.mGlibc != null ? this.mGlibc.hashCode() : 0);
        hash = 53 * hash + (this.mInstalled ? 1 : 0);
        return hash;
    }

    @Override
    public String toString() {
        return mNamespace + "::" + mName + ":" + mVersion;
    }

    @Override
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    @Override
    public void toXML(XMLWriter writer, int indent) {
        if (mProfiles.isEmpty() && mPFNs.isEmpty() && mMetadata.isEmpty()) {
            mLogger.log(
                    "The executable element for "
                            + mName
                            + " must have atleast 1 profile, 1 pfn or 1 metadata entry. Skipping empty executable element",
                    LogManager.WARNING_MESSAGE_LEVEL);
        } else {
            writer.startElement("executable", indent);
            if (mNamespace != null && !mNamespace.isEmpty()) {
                writer.writeAttribute("namespace", mNamespace);
            }
            writer.writeAttribute("name", mName);
            if (mVersion != null && !mVersion.isEmpty()) {
                writer.writeAttribute("version", mVersion);
            }
            if (mInstalled) {
                writer.writeAttribute("installed", "true");
            } else {
                writer.writeAttribute("installed", "false");
            }
            if (mArch != null) {
                writer.writeAttribute("arch", mArch.toString().toLowerCase());
            }
            if (mOs != null) {
                writer.writeAttribute("os", mOs.toString().toLowerCase());
            }
            if (mOsRelease != null && !mOsRelease.isEmpty()) {
                writer.writeAttribute("osrelease", mOsRelease);
            }
            if (mOsVersion != null && !mOsVersion.isEmpty()) {
                writer.writeAttribute("osversion", mOsVersion);
            }
            if (mGlibc != null && !mGlibc.isEmpty()) {
                writer.writeAttribute("glibc", mGlibc);
            }
            super.toXML(writer, indent);
            for (Invoke i : mInvokes) {
                i.toXML(writer, indent + 1);
            }
            writer.endElement(indent);
        }
    }

    /**
     * Converts the executable into transformation catalog entries
     *
     * @return transformation catalog entries
     */
    public List<TransformationCatalogEntry> toTransformationCatalogEntries() {
        List<TransformationCatalogEntry> tceList = new ArrayList<>();
        for (PFN pfn : this.getPhysicalFiles()) {

            TransformationCatalogEntry tce =
                    new TransformationCatalogEntry(
                            this.getNamespace(), this.getName(), this.getVersion());
            SysInfo sysinfo = new SysInfo();
            sysinfo.setArchitecture(
                    SysInfo.Architecture.valueOf(this.getArchitecture().toString().toLowerCase()));
            sysinfo.setOS(SysInfo.OS.valueOf(this.getOS().toString().toLowerCase()));
            sysinfo.setOSRelease(this.getOsRelease());
            sysinfo.setOSVersion(this.getOsVersion());
            sysinfo.setGlibc(this.getGlibc());
            tce.setSysInfo(sysinfo);
            tce.setType(this.getInstalled() ? TCType.INSTALLED : TCType.STAGEABLE);
            tce.setResourceId(pfn.getSite());
            tce.setPhysicalTransformation(pfn.getURL());

            for (Executable e : this.mRequires) {
                tce.addRequirement(e);
            }

            Notifications notifications = new Notifications();
            for (Invoke invoke : this.getInvoke()) {
                notifications.add(new Invoke(invoke));
            }
            tce.addNotifications(notifications);
            for (edu.isi.pegasus.planner.dax.Profile profile : this.getProfiles()) {
                tce.addProfile(
                        new edu.isi.pegasus.planner.classes.Profile(
                                profile.getNameSpace(), profile.getKey(), profile.getValue()));
            }
            for (MetaData md : this.getMetaData()) {
                // convert to metadata profile object for planner to use
                tce.addProfile(
                        new edu.isi.pegasus.planner.classes.Profile(
                                edu.isi.pegasus.planner.classes.Profile.METADATA,
                                md.getKey(),
                                md.getValue()));
            }
            tceList.add(tce);
        }

        return tceList;
    }
}
