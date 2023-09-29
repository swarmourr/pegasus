/**
 * Copyright 2007-2012 University Of Southern California
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.XMLWriter;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author gmehta
 * @version $Revision$
 */
@JsonSerialize(using = AbstractJob.JsonSerializer.class)
public class AbstractJob {

    protected List mArguments;
    protected List<Profile> mProfiles;
    protected File mStdin;
    protected File mStdout;
    protected File mStderr;
    protected Set<File> mUses;
    protected List<Invoke> mInvokes;
    protected String mName;
    protected String mId;
    protected String mNamespace;
    protected String mVersion;
    protected String mNodeLabel;
    /** The metadata attributes associated with the whole workflow. */
    private Set<MetaData> mMetaDataAttributes;

    protected static LogManager mLogger;
    private static final String ARG_DELIMITER = " ";
    private static final String FILE_DELIMITER = " ";
    private static final String JOBTYPE = "AbstractJob";

    protected AbstractJob() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mArguments = new LinkedList();
        mUses = new LinkedHashSet<File>();
        mInvokes = new LinkedList<Invoke>();
        mProfiles = new LinkedList<Profile>();
        mMetaDataAttributes = new LinkedHashSet<MetaData>();
    }

    /**
     * Copy constructor
     *
     * @param a the job
     */
    protected AbstractJob(AbstractJob a) {
        this.mArguments = new LinkedList(a.mArguments);
        this.mProfiles = new LinkedList<Profile>(a.mProfiles);
        this.mStdin = new File(a.mStdin);
        this.mStdout = new File(a.mStdout);
        this.mStderr = new File(a.mStderr);
        this.mUses = new LinkedHashSet<File>(a.mUses);
        this.mMetaDataAttributes = new LinkedHashSet<MetaData>(a.mMetaDataAttributes);
        this.mInvokes = new LinkedList<Invoke>(a.mInvokes);
        this.mName = a.mName;
        this.mId = a.mId;
        this.mNamespace = a.mNamespace;
        this.mVersion = a.mVersion;
        this.mNodeLabel = a.mNodeLabel;
    }

    /**
     * Copy Constructor
     *
     * @param id the string id
     */
    protected static void checkID(String id) {
        if (!Patterns.isNodeIdValid(id)) {
            mLogger.log(
                    "Id: " + id + " should of the type [A-Za-z0-9][-A-Za-z0-9]*",
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
    }

    /**
     * Return the argument List. The List contains both {@link String} as well as {@link File}
     * objects
     *
     * @return List
     */
    public List getArguments() {
        return mArguments;
    }

    /**
     * Add a string argument to the argument List. Each call to argument adds a space in between
     * entries
     *
     * @param argument argument to the job
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argument) {
        if (argument != null) {
            mArguments.add(argument);
        }
        return this;
    }

    /**
     * Add a file object to the argument List. Each call to argument adds a space between entries.
     *
     * @param file the file
     * @return AbstractJob
     * @see File
     */
    public AbstractJob addArgument(File file) {
        if (file != null) {
            mArguments.add(file);
        }
        return this;
    }

    /**
     * Add a Array of {@link File} objects to the argument list. The files will be separated by
     * space when rendered on the command line
     *
     * @param files File[]
     * @return AbstractJob
     * @see File
     */
    public AbstractJob addArgument(File[] files) {
        for (File f : files) {
            this.addArgument(f);
        }
        return this;
    }

    /**
     * Add a List of {@link File} objects to the argument list. The files will be separated by space
     * when rendered on the command line
     *
     * @param files List of File objects
     * @return AbstractJob
     * @see File
     */
    public AbstractJob addArgument(List<File> files) {
        for (File f : files) {
            this.addArgument(f);
        }
        return this;
    }

    /**
     * Add a Array of {@link File} objects to the argument list. The files will be separated by the
     * filedelimiter(default is space) when rendered on the command line.
     *
     * @param files File[] Array of file objects
     * @param filedelimiter String delimiter for the files. Default is space
     * @return AbstractJob
     * @see File
     * @deprecated As of 5.0.2 release this function will not work consistently as ARG_LIMITER is
     *     always added between args
     */
    public AbstractJob addArgument(File[] files, String filedelimiter) {
        filedelimiter = (filedelimiter == null) ? FILE_DELIMITER : filedelimiter;
        if (files != null && files.length > 0) {
            if (!mArguments.isEmpty()) {
                mArguments.add(ARG_DELIMITER);
            }
            boolean first = true;
            for (File f : files) {
                if (!first) {
                    mArguments.add(filedelimiter);
                }
                mArguments.add(f);
                first = false;
            }
        }
        return this;
    }

    /**
     * Add a List of {@link File} objects to the argument list. The files will be separated by the
     * filedelimiter(default is space) when rendered on the command line.
     *
     * @param files List of File objects
     * @param filedelimiter String delimiter for the files. Default is space
     * @return AbstractJob
     * @see File
     * @deprecated As of 5.0.2 release this function will not work consistently as ARG_LIMITER is
     *     always added between args
     */
    public AbstractJob addArgument(List<File> files, String filedelimiter) {
        if (files != null && !files.isEmpty()) {
            this.addArgument((File[]) files.toArray(), filedelimiter);
        }
        return this;
    }

    /**
     * Add a argument key and value to the argument List. The argkey and argvalue are seperated by
     * space. Example addArgument("-p","0") will result in the argument being added as -p 0<br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue String
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argkey, String argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER);
        return this;
    }

    /**
     * Add a argument key and value to the argument List.<br>
     * The argkey and argvalue are seperated by argdelimiter.<br>
     * Example addArgument("-p","0","=") will result in the argument being added as -p=0<br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String Key
     * @param argvalue String Value
     * @param argdelimiter String argdelimiter
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argkey, String argvalue, String argdelimiter) {
        argdelimiter = (argdelimiter == null) ? ARG_DELIMITER : argdelimiter;
        if (argkey != null && argvalue != null) {
            this.addArgument(argkey + argdelimiter + argvalue);
        }
        return this;
    }

    /**
     * Add a argument key and File value to the argument List.<br>
     * The argkey and argvalue are seperated by space.<br>
     * Example addArgument("-i",new File("f.a")) will result in the argument being added as -i
     * &lt;file name="f.a"&gt;<br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue File
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argkey, File argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER);
        return this;
    }

    /**
     * Add a argument key and File value to the argument List.<br>
     * The argkey and argvalue are separated by the argdelimiter.<br>
     * Example addArgument("-i",new File("f.a"),"=") will result in the argument being added as
     * -i=&lt;file name="f.a"&gt;<br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue File
     * @param argdelimiter argumenet delimiter
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argkey, File argvalue, String argdelimiter) {
        argdelimiter = (argdelimiter == null) ? ARG_DELIMITER : argdelimiter;
        if (argkey != null && argvalue != null) {
            this.addArgument(argkey + argdelimiter);
            mArguments.add(argvalue);
        }
        return this;
    }

    /**
     * Add a argument key and an array of Files to the argument List.<br>
     * The argkey and argvalue are separated space.<br>
     * The files are separated by a space <br>
     * Example:<br>
     * <i>File[] files = {new File("f.a1"), new File("f.a2")};<br>
     * job.addArgument("-i",files)</i><br>
     * will result in the argument being added as <b>-i &lt;file name="f.a1"&gt; &lt;file
     * name="f.a2"&gt;</b><br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue File[]
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argkey, File[] argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER, FILE_DELIMITER);
        return this;
    }

    /**
     * Add a argument key and a List of Files to the argument List.<br>
     * The argkey and argvalue are separated space.<br>
     * The files are separated by a space <br>
     * Example:<br>
     *
     * <pre>{@code
     * List<File> files = new LinkedList<File>();
     * files.add(new File("f.a1"));
     * files.add(new File("f.a2"));
     * job.addArgument("-i",files)
     * }
     * will result in the argument being added as <b>-i &lt;file name="f.a1"&gt; &lt;file
     * name="f.a2"&gt;</b><br>
     * </pre>
     *
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue list of files to be passed as arguments
     * @return AbstractJob
     */
    public AbstractJob addArgument(String argkey, List<File> argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER, FILE_DELIMITER);
        return this;
    }

    /**
     * Add a argument key and an array of Files to the argument List.<br>
     * The argkey and argvalue are separated by the argdelimiter.<br>
     * The files are separated by a filedelimiter <br>
     * Example:<br>
     * <i>File[] files = {new File("f.a1"), new File("f.a2")};<br>
     * job.addArgument("-i",files,"=",",")</i><br>
     * will result in the argument being added as <b>-i=&lt;file name="f.a1"&gt;,&lt;file
     * name="f.a2"&gt;</b><br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue File[]
     * @param argdelimiter String
     * @param filedelimiter String
     * @return AbstractJob
     */
    public AbstractJob addArgument(
            String argkey, File[] argvalue, String argdelimiter, String filedelimiter) {
        argdelimiter = (argdelimiter == null) ? ARG_DELIMITER : argdelimiter;
        filedelimiter = (filedelimiter == null) ? FILE_DELIMITER : filedelimiter;

        if (argkey != null && argvalue != null && argvalue.length > 0) {
            this.addArgument(argkey + argdelimiter);
            boolean first = true;
            for (File f : argvalue) {
                if (!first) {
                    mArguments.add(filedelimiter);
                }
                mArguments.add(f);
                first = false;
            }
        }
        return this;
    }

    /**
     * Add a argument key and a List of Files to the argument List.<br>
     * The argkey and argvalue are separated by the argdelimiter.<br>
     * The files are separated by a filedelimter <br>
     * Example:<br>
     * {@code List<File> files = new LinkedList<File>(); files.add(new File("f.a1")); files.add(new
     * File("f.a2")); job.addArgument("-i",files,"=",",") } will result in the argument being added
     * as <b>-i=&lt;file name="f.a1"&gt;,&lt;file name="f.a2"&gt;</b><br>
     * Multiple calls to addArgument results in the arguments being separated by space.
     *
     * @param argkey String
     * @param argvalue List&lt;File&gt; List of File objects
     * @param argdelimiter String
     * @param filedelimiter String
     * @return AbstractJob
     */
    public AbstractJob addArgument(
            String argkey, List<File> argvalue, String argdelimiter, String filedelimiter) {
        if (argkey != null && argvalue != null && !argvalue.isEmpty()) {
            this.addArgument(argkey, (File[]) argvalue.toArray(), argdelimiter, filedelimiter);
        }
        return this;
    }

    /**
     * Add a profile to the job
     *
     * @param namespace String
     * @param key String
     * @param value String
     * @return AbstractJob
     */
    public AbstractJob addProfile(String namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    /**
     * Add a profile to the job
     *
     * @param namespace {@link Profile.NAMESPACE}
     * @param key String
     * @param value String
     * @return AbstractJob
     */
    public AbstractJob addProfile(Profile.NAMESPACE namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    /**
     * Add a Profile object
     *
     * @param profile the profile to add
     * @return AbstractJob
     * @see Profile
     */
    public AbstractJob addProfile(Profile profile) {
        mProfiles.add(profile);
        return this;
    }

    /**
     * Add a list of Profile objects
     *
     * @param profiles List&lt;Profile&gt;
     * @return AbstractJob
     */
    public AbstractJob addProfiles(List<Profile> profiles) {
        mProfiles.addAll(profiles);
        return this;
    }

    /**
     * Return the profile List. The List contains both {@link Profile} objects
     *
     * @return List
     */
    public List getProfiles() {
        return mProfiles;
    }

    /**
     * Get the STDIN file object
     *
     * @return File
     */
    public File getStdin() {
        return mStdin;
    }

    /**
     * @param stdin file object corresponding to the stdin of the file
     * @return AbstractJob
     */
    public AbstractJob setStdin(File stdin) {
        File f = new File(stdin, File.LINK.INPUT);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin file object corresponding to the stdin of the file
     * @param transfer the transfer flag to associate with stdin.
     * @return AbstractJob
     */
    public AbstractJob setStdin(File stdin, File.TRANSFER transfer) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param register the register flag
     * @return AbstractJob
     */
    public AbstractJob setStdin(File stdin, boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setRegister(register);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param transfer the transfer flag for stdin
     * @param register the register flag for the stdin
     * @return AbstractJob
     */
    public AbstractJob setStdin(File stdin, File.TRANSFER transfer, boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param transfer the transfer flag for stdin
     * @param register the register flag for the stdin
     * @param optional boolean indicating file is optional
     * @return AbstractJob
     */
    public AbstractJob setStdin(
            File stdin, File.TRANSFER transfer, boolean register, boolean optional) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @return AbstractJob
     */
    public AbstractJob setStdin(String stdin) {
        File f = new File(stdin, File.LINK.INPUT);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param transfer transfer flag associated with the stdin
     * @return AbstractJob
     */
    public AbstractJob setStdin(String stdin, File.TRANSFER transfer) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param register the register flag
     * @return AbstractJob
     */
    public AbstractJob setStdin(String stdin, boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setRegister(register);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param transfer the transfer flag for stdin
     * @param register boolean register flag for the stdin
     * @return AbstractJob
     */
    public AbstractJob setStdin(String stdin, File.TRANSFER transfer, boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdin = f;
        return this;
    }

    /**
     * @param stdin File object corresponding to the stdin of the file
     * @param transfer the transfer flag for stdin
     * @param register boolean register flag for the stdin
     * @param optional boolean indicating whether file is optional or not.
     * @return AbstractJob
     */
    public AbstractJob setStdin(
            String stdin, File.TRANSFER transfer, boolean register, boolean optional) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdin = f;
        return this;
    }

    /** @return File */
    public File getStdout() {
        return mStdout;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @return AbstractJob
     */
    public AbstractJob setStdout(File stdout) {
        File f = new File(stdout, File.LINK.OUTPUT);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param transfer transfer flag
     * @return AbstractJob
     */
    public AbstractJob setStdout(File stdout, File.TRANSFER transfer) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param register boolean to indicate whether to register stdout or not
     * @return AbstractJob
     */
    public AbstractJob setStdout(File stdout, boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setRegister(register);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param transfer transfer flag for stdout
     * @param register boolean to indicate whether to register stdout or not
     * @return AbstractJob
     */
    public AbstractJob setStdout(File stdout, File.TRANSFER transfer, boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param transfer transfer flag for stdout
     * @param register boolean to indicate whether to register stdout or not
     * @param optional boolean to indicate whether file is optional or not.
     * @return AbstractJob
     */
    public AbstractJob setStdout(
            File stdout, File.TRANSFER transfer, boolean register, boolean optional) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout name of the file to which stdout should be redirected to
     * @return AbstractJob
     */
    public AbstractJob setStdout(String stdout) {
        File f = new File(stdout, File.LINK.OUTPUT);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param transfer transfer flag for stdout
     * @return AbstractJob
     */
    public AbstractJob setStdout(String stdout, File.TRANSFER transfer) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param register boolean to indicate whether to register stdout or not
     * @return AbstractJob
     */
    public AbstractJob setStdout(String stdout, boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setRegister(register);
        mStdout = f;
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param transfer transfer flag for stdout
     * @param register boolean to indicate whether to register stdout or not
     * @return AbstractJob
     */
    public AbstractJob setStdout(String stdout, File.TRANSFER transfer, boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdout = f;
        mUses.add(f);
        return this;
    }

    /**
     * @param stdout File object corresponding to the stdout of the job
     * @param transfer transfer flag for stdout
     * @param register boolean to indicate whether to register stdout or not
     * @param optional indicate whether file is optional or not.
     * @return AbstractJob
     */
    public AbstractJob setStdout(
            String stdout, File.TRANSFER transfer, boolean register, boolean optional) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdout = f;
        return this;
    }

    /** @return File */
    public File getStderr() {
        return mStderr;
    }

    /**
     * @param stderr File object corresponding to the stderr of the job
     * @return AbstractJob
     */
    public AbstractJob setStderr(File stderr) {
        File f = new File(stderr, File.LINK.OUTPUT);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr File object corresponding to the stderr of the job
     * @param transfer transfer flag for stderr
     * @return AbstractJob
     */
    public AbstractJob setStderr(File stderr, File.TRANSFER transfer) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr File object corresponding to the stderr of the job
     * @param register boolean to indicate whether to register stederr or not
     * @return AbstractJob
     */
    public AbstractJob setStderr(File stderr, boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setRegister(register);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr File object corresponding to the stderr of the job
     * @param transfer transfer flag for stderr
     * @param register boolean to indicate whether to register stederr or not
     * @return AbstractJob
     */
    public AbstractJob setStderr(File stderr, File.TRANSFER transfer, boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr File object corresponding to the stderr of the job
     * @param transfer transfer flag for stderr
     * @param register boolean to indicate whether to register stederr or not
     * @param optional boolean to indicate whether file is optional or not
     * @return AbstractJob
     */
    public AbstractJob setStderr(
            File stderr, File.TRANSFER transfer, boolean register, boolean optional) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr name of file to which stderr of the job should be redirected to
     * @return AbstractJob
     */
    public AbstractJob setStderr(String stderr) {
        File f = new File(stderr, File.LINK.OUTPUT);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr name of the file to which stderr should be redirected to
     * @param transfer transfer flag for stderr
     * @return AbstractJob
     */
    public AbstractJob setStderr(String stderr, File.TRANSFER transfer) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr name of the file to which stderr should be redirected to
     * @param register boolean to indicate whether to register stederr or not
     * @return AbstractJob
     */
    public AbstractJob setStderr(String stderr, boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setRegister(register);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr name of the file to which stderr should be redirected to
     * @param transfer transfer flag for stderr
     * @param register boolean to indicate whether to register stederr or not
     * @return AbstractJob
     */
    public AbstractJob setStderr(String stderr, File.TRANSFER transfer, boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStderr = f;
        return this;
    }

    /**
     * @param stderr name of the file to which stderr should be redirected to
     * @param transfer transfer flag for stderr
     * @param register boolean to indicate whether to register stederr or not
     * @param optional boolean indicating whether file is optional or not
     * @return AbstractJob
     */
    public AbstractJob setStderr(
            String stderr, File.TRANSFER transfer, boolean register, boolean optional) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStderr = f;
        return this;
    }

    /** @return Set of File objects */
    public Set<File> getUses() {
        return mUses;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link) {
        File f = new File(file, link);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param register boolean indicating whether file should be registered or not
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link, boolean register) {
        File f = new File(file, link);
        f.setRegister(register);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param register boolean indicating whether file should be registered or not
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link, boolean register, String size) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link, String size) {
        File f = new File(file, link);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link, File.TRANSFER transfer) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link, File.TRANSFER transfer, String size) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file or not
     * @return AbstractJob
     */
    public AbstractJob uses(String file, File.LINK link, File.TRANSFER transfer, boolean register) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setTransfer(transfer);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file or not
     * @param forPlanning whether the file is required for use by planner
     * @return AbstractJob
     */
    public AbstractJob uses(
            String file,
            File.LINK link,
            File.TRANSFER transfer,
            boolean register,
            boolean forPlanning) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setTransfer(transfer);
        f.setUseForPlanning(forPlanning);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register file or not
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(
            String file, File.LINK link, File.TRANSFER transfer, boolean register, String size) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setTransfer(transfer);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @param optional whether file is optional or not
     * @param executable whether file being transferred is an executable or not
     * @return AbstractJob
     */
    public AbstractJob uses(
            String file,
            File.LINK link,
            File.TRANSFER transfer,
            boolean register,
            boolean optional,
            boolean executable) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setOptional(optional);
        f.setTransfer(transfer);
        f.setExecutable(executable);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @param optional whether file is optional or not
     * @param executable whether file being transferred is an executable or not
     * @param size size
     * @return AbstractJob
     */
    public AbstractJob uses(
            String file,
            File.LINK link,
            File.TRANSFER transfer,
            boolean register,
            boolean optional,
            boolean executable,
            String size) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setOptional(optional);
        f.setTransfer(transfer);
        f.setExecutable(executable);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link) {
        File f = new File(file, link);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link, File.TRANSFER transfer) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link, File.TRANSFER transfer, String size) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param register whether to register the file
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link, boolean register) {
        File f = new File(file, link);
        f.setRegister(register);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param register register flag
     * @param size size
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link, boolean register, String size) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param size size
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link, String size) {
        File f = new File(file, link);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @return AbstractJob
     */
    public AbstractJob uses(File file, File.LINK link, File.TRANSFER transfer, boolean register) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @param forPlanning whether the file is to be used for planning purposes
     * @return AbstractJob
     */
    public AbstractJob uses(
            File file,
            File.LINK link,
            File.TRANSFER transfer,
            boolean register,
            boolean forPlanning) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setUseForPlanning(forPlanning);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(
            File file, File.LINK link, File.TRANSFER transfer, boolean register, String size) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @param optional whether file is optional or not
     * @param executable whether file being transferred is an executable or not
     * @return AbstractJob
     */
    public AbstractJob uses(
            File file,
            File.LINK link,
            File.TRANSFER transfer,
            boolean register,
            boolean optional,
            boolean executable) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        f.setExecutable(executable);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param file the file used by the job
     * @param link indicating whether file is input or output
     * @param transfer transfer flag indicating whether file should be transferred or not
     * @param register whether to register the file
     * @param optional whether file is optional or not
     * @param executable whether file being transferred is an executable or not
     * @param size size of the file
     * @return AbstractJob
     */
    public AbstractJob uses(
            File file,
            File.LINK link,
            File.TRANSFER transfer,
            boolean register,
            boolean optional,
            boolean executable,
            String size) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        f.setExecutable(executable);
        f.setSize(size);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log(
                    "Job "
                            + Separator.combine(mNamespace, mName, mVersion)
                            + "already contains a file "
                            + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                            + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * @param files list of files to use
     * @param link linkage for the file
     * @return AbstractJob
     */
    public AbstractJob uses(List<File> files, File.LINK link) {
        for (File file : files) {
            File f = new File(file, link);
            if (!mUses.contains(f)) {
                mUses.add(f);
            } else {
                mLogger.log(
                        "Job "
                                + Separator.combine(mNamespace, mName, mVersion)
                                + "already contains a file "
                                + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                                + ". Ignoring",
                        LogManager.WARNING_MESSAGE_LEVEL);
            }
        }
        return this;
    }

    /**
     * Returns list of Invoke objects
     *
     * @return List of invoke objects
     */
    public List<Invoke> getInvoke() {
        return mInvokes;
    }

    /**
     * Same as getInvoke(). Returns list of Invoke objects
     *
     * @return List of invoke objects containing the notifications
     */
    public List<Invoke> getNotification() {
        return getInvoke();
    }

    /**
     * Add Notification to the job
     *
     * @param when when to invoke
     * @param what what executable to invoke including the arguments
     * @return AbstractJob
     */
    public AbstractJob addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    /**
     * Add Notification to the job
     *
     * @param when when to invoke
     * @param what what executable to invoke including the arguments
     * @return AbstractJob
     */
    public AbstractJob addNotification(Invoke.WHEN when, String what) {
        return addInvoke(when, what);
    }

    /**
     * Add notification to the job
     *
     * @param invoke the Invoke object
     * @return AbstractJob
     */
    public AbstractJob addInvoke(Invoke invoke) {
        mInvokes.add(invoke.clone());
        return this;
    }

    /**
     * Add notification to the job
     *
     * @param invoke the Invoke object
     * @return AbstractJob
     */
    public AbstractJob addNotification(Invoke invoke) {
        return addInvoke(invoke);
    }

    /**
     * Add Notifications to the job
     *
     * @param invokes list of invoke objects
     * @return AbstractJob
     */
    public AbstractJob addInvokes(List<Invoke> invokes) {
        for (Invoke invoke : invokes) {
            this.addInvoke(invoke);
        }
        return this;
    }

    /**
     * Add Notifications to the job
     *
     * @param invokes list of Invoke objects
     * @return AbstractJob
     */
    public AbstractJob addNotifications(List<Invoke> invokes) {
        return addInvokes(invokes);
    }

    /**
     * Adds metadata to the workflow
     *
     * @param key key name for metadata
     * @param value value
     * @return the Abstract Job
     */
    public AbstractJob addMetaData(String key, String value) {
        this.mMetaDataAttributes.add(new MetaData(key, value));
        return this;
    }

    /**
     * Returns the metadata associated for a key if exists, else null
     *
     * @param key the metadata key
     * @return metadata key
     */
    public String getMetaData(String key) {
        return this.mMetaDataAttributes.contains(key)
                ? ((MetaData) mMetaDataAttributes).getValue()
                : null;
    }

    /**
     * Is this Object a Job
     *
     * @return boolean
     */
    public boolean isJob() {
        return false;
    }

    /**
     * Is this Object a DAX
     *
     * @return boolean
     */
    public boolean isDAX() {
        return false;
    }

    /**
     * Is this Object a DAG
     *
     * @return boolean
     */
    public boolean isDAG() {
        return false;
    }

    /** @return String */
    public String getName() {
        return mName;
    }

    /** @return String */
    public String getId() {
        return mId;
    }

    /** @return String */
    public String getNodeLabel() {
        return mNodeLabel;
    }

    /** @param label node lable */
    public void setNodeLabel(String label) {
        this.mNodeLabel = label;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AbstractJob other = (AbstractJob) obj;
        if ((this.mId == null) ? (other.mId != null) : !this.mId.equals(other.mId)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + (this.mId != null ? this.mId.hashCode() : 0);
        return hash;
    }

    /** @param writer the XMLWriter */
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    /**
     * @param writer the XMLWriter
     * @param indent indentation
     */
    public void toXML(XMLWriter writer, int indent) {

        // Check if its a dax, dag or job class

        if (mNodeLabel != null && !mNodeLabel.isEmpty()) {
            writer.writeAttribute("node-label", mNodeLabel);
        } // add argument

        if (!mArguments.isEmpty()) {
            writer.startElement("argument", indent + 1);
            boolean addArgLimiter = false;
            for (Object o : mArguments) {
                if (addArgLimiter) {
                    writer.writeData(ARG_DELIMITER);
                }
                if (o.getClass() == String.class) {
                    // if class is string add argument string in the data section
                    writer.writeData((String) o);
                }
                if (o.getClass() == File.class) {
                    // add file tags in the argument elements data section
                    ((File) o).toXML(writer, 0, "argument");
                }
                addArgLimiter = true;
            }
            writer.endElement();
        }

        // PM-902
        for (MetaData md : mMetaDataAttributes) {
            md.toXML(writer, indent + 1);
        }

        // add profiles
        for (Profile p : mProfiles) {
            p.toXML(writer, indent + 1);
        }

        // PM-708 add extra uses for stdout|stderr|stdin if not
        // specified by the user in the uses section
        Set<File> addOnUses = new LinkedHashSet<File>();

        // add stdin
        if (mStdin != null) {
            if (!mUses.contains(mStdin)) {
                // add uses with default flags
                File f = new File(mStdin, File.LINK.INPUT);
                addOnUses.add(f);
            }
            mStdin.toXML(writer, indent + 1, "stdin");
        }

        // add stdout
        if (mStdout != null) {
            if (!mUses.contains(mStdout)) {
                // add uses with default flags
                File f = new File(mStdout, File.LINK.OUTPUT);
                addOnUses.add(f);
            }
            mStdout.toXML(writer, indent + 1, "stdout");
        }

        // add stderr
        if (mStderr != null) {
            if (!mUses.contains(mStderr)) {
                // add uses with default flags
                File f = new File(mStderr, File.LINK.OUTPUT);
                addOnUses.add(f);
            }
            mStderr.toXML(writer, indent + 1, "stderr");
        }

        // print add on uses first
        for (File f : addOnUses) {
            f.toXML(writer, indent + 1, "uses");
        }

        // add uses
        for (File f : mUses) {
            f.toXML(writer, indent + 1, "uses");
        } // add invoke
        for (Invoke i : mInvokes) {
            i.toXML(writer, indent + 1);
        }
        if (!(mUses.isEmpty()
                && mInvokes.isEmpty()
                && mStderr == null
                && mStdout == null
                && mStdin == null
                && mProfiles.isEmpty()
                && mArguments.isEmpty())) {
            writer.endElement(indent);
        } else {
            writer.endElement();
        }
    }

    /**
     * Custom serializer for YAML representation of AbstractJob
     *
     * @author Ryan Tanaka
     */
    public static class JsonSerializer extends PegasusJsonSerializer<AbstractJob> {

        public JsonSerializer() {}

        public void serialize(AbstractJob job, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();

            // am I a job, DAG, or DAX
            if (job.isJob()) {
                gen.writeStringField("type", "job");
                gen.writeStringField("name", job.mName);
                // namespace
                if (job.mNamespace != null && !job.mNamespace.trim().isEmpty()) {
                    gen.writeStringField("namespace", job.mNamespace);
                }

                // version
                if (job.mVersion != null && !job.mVersion.trim().isEmpty()) {
                    gen.writeStringField("version", job.mVersion);
                }
            } else if (job.isDAG()) {
                gen.writeStringField("type", "condorWorkflow");
                gen.writeStringField("file", job.mName);
            } else {
                gen.writeStringField("type", "pegasusWorkflow");
                gen.writeStringField("file", job.mName);
            }

            // id
            gen.writeStringField("id", job.mId);

            // nodeLabel
            if (job.mNodeLabel != null && !job.mNodeLabel.isEmpty()) {
                gen.writeStringField("nodeLabel", job.mNodeLabel);
            }

            // profiles
            if (!job.mProfiles.isEmpty()) {
                // group profiles by namespace
                Map<String, List<Profile>> profiles = new HashMap<>();
                for (Profile.NAMESPACE nspc : Profile.NAMESPACE.values()) {
                    profiles.put(nspc.toString().toLowerCase(), new ArrayList<>());
                }

                for (Profile p : job.mProfiles) {
                    profiles.get(p.getNameSpace().toLowerCase()).add(p);
                }

                gen.writeObjectFieldStart("profiles");
                for (Map.Entry<String, List<Profile>> e : profiles.entrySet()) {
                    if (!e.getValue().isEmpty()) {
                        gen.writeObjectFieldStart(e.getKey());
                        for (Profile p : e.getValue()) {
                            gen.writeStringField(p.getKey(), p.getValue());
                        }
                        gen.writeEndObject();
                    }
                }
                gen.writeEndObject();
            }

            // metadata
            if (!job.mMetaDataAttributes.isEmpty()) {
                gen.writeArrayFieldStart("metadata");
                for (MetaData m : job.mMetaDataAttributes) {
                    gen.writeObject(m);
                }
                gen.writeEndArray();
            }

            // hooks
            if (!job.mInvokes.isEmpty()) {
                gen.writeObjectFieldStart("hooks");
                gen.writeArrayFieldStart("shell");
                for (Invoke iv : job.mInvokes) {
                    gen.writeObject(iv);
                }
                gen.writeEndArray();
                gen.writeEndObject();
            }

            // add extra uses for stdin|stdout|stderr if not specified
            // by the user in the uses section
            Set<File> addOnUses = new LinkedHashSet<File>();

            // stdin
            if (job.mStdin != null) {
                if (!job.mUses.contains(job.mStdin)) {
                    File f = new File(job.mStdin, File.LINK.INPUT);
                    addOnUses.add(f);
                }

                gen.writeStringField("stdin", job.mStdin.mName);
            }

            // stdout
            if (job.mStdout != null) {
                if (!job.mUses.contains(job.mStdout)) {
                    File f = new File(job.mStdout, File.LINK.OUTPUT);
                    addOnUses.add(f);
                }

                gen.writeStringField("stdout", job.mStdout.mName);
            }

            // stderr
            if (job.mStderr != null) {
                if (!job.mUses.contains(job.mStderr)) {
                    File f = new File(job.mStderr, File.LINK.OUTPUT);
                    addOnUses.add(f);
                }

                gen.writeStringField("stderr", job.mStderr.mName);
            }

            // arguments
            gen.writeArrayFieldStart("arguments");
            for (Object o : job.mArguments) {
                if (o.getClass() == String.class) {
                    gen.writeString((String) o);
                }

                if (o.getClass() == File.class) {
                    gen.writeString(((File) o).mName);
                }
            }
            gen.writeEndArray();

            // uses
            gen.writeArrayFieldStart("uses");
            for (File f : job.mUses) {
                gen.writeObject(f);
            }
            gen.writeEndArray();

            gen.writeEndObject();
        }
    }
}
