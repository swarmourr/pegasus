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
package edu.isi.pegasus.planner.catalog.replica;

import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FileDetector;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.IOException;
import java.lang.reflect.*;
import java.util.Enumeration;
import java.util.Properties;

/**
 * This factory loads a replica catalog, as specified by the properties. Each invocation of the
 * factory will result in a new instance of a connection to the replica catalog.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalog
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
 * @see edu.isi.pegasus.planner.catalog.replica.impl.JDBCRC
 */
public class ReplicaFactory {

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PACKAGE = "edu.isi.pegasus.planner.catalog.replica.impl";

    public static final String DEFAULT_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.replica.impl.YAML.class.getCanonicalName();

    public static final String YAML_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.replica.impl.YAML.class.getCanonicalName();

    public static final String FILE_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.replica.impl.SimpleFile.class.getCanonicalName();

    /**
     * Connects the interface with the replica catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param props is an instance of properties to use.
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     * @see org.griphyn.common.util.CommonProperties
     * @see #loadInstance()
     */
    public static ReplicaCatalog loadInstance(PegasusProperties props)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {

        return loadInstance(props, props.getPropertiesInSubmitDirectory());
    }

    /**
     * Connects the interface with the replica catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param props is an instance of properties to use.
     * @param file the physical location of the property file
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     * @see org.griphyn.common.util.CommonProperties
     * @see #loadInstance()
     */
    public static ReplicaCatalog loadInstance(PegasusProperties props, String file)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {

        // sanity check

        if (props == null) throw new NullPointerException("invalid properties");

        Properties connect = props.matchingSubset(ReplicaCatalog.c_prefix, false);

        // get the default db driver properties in first pegasus.catalog.*.db.driver.*
        Properties db = props.matchingSubset(ReplicaCatalog.DB_ALL_PREFIX, false);
        // now overload with the work catalog specific db properties.
        // pegasus.catalog.work.db.driver.*
        db.putAll(props.matchingSubset(ReplicaCatalog.DB_PREFIX, false));

        // PM-778 properties file location requried for pegasus-db-admin
        if (file != null) {
            connect.put("properties.file", file);
        }

        // to make sure that no confusion happens.
        // add the db prefix to all the db properties
        for (Enumeration e = db.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            connect.put("db." + key, db.getProperty(key));
        }

        // determine the class that implements the work catalog
        return loadInstance(props.getProperty(ReplicaCatalog.c_prefix), connect);
    }

    /**
     * Connects the interface with the replica catalog implementation.The choice of backend is
     * configured through properties.This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param catalogImplementor
     * @param props is an instance of properties to use.
     * @return
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     * @throws java.io.IOException
     * @see org.griphyn.common.util.CommonProperties
     * @see #loadInstance()
     */
    public static ReplicaCatalog loadInstance(String catalogImplementor, Properties props)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        ReplicaCatalog result = null;

        if (catalogImplementor == null) {
            // check if file is specified in properties
            if (props.containsKey("file")) {
                // PM-1518 check for type of file
                if (FileDetector.isTypeYAML(props.getProperty("file"))) {
                    catalogImplementor = YAML_CATALOG_IMPLEMENTOR;
                } else {
                    catalogImplementor = FILE_CATALOG_IMPLEMENTOR;
                }
            } else {
                catalogImplementor = DEFAULT_CATALOG_IMPLEMENTOR;
            }
        }

        // File also means SimpleFile
        if (catalogImplementor.equalsIgnoreCase("File")) {
            catalogImplementor = FILE_CATALOG_IMPLEMENTOR;
        }

        // syntactic sugar adds absolute class prefix
        if (catalogImplementor.indexOf('.') == -1)
            catalogImplementor = DEFAULT_PACKAGE + "." + catalogImplementor;
        // POSTCONDITION: we have now a fully-qualified classname

        DynamicLoader dl = new DynamicLoader(catalogImplementor);
        result = (ReplicaCatalog) dl.instantiate(new Object[0]);
        if (result == null) throw new RuntimeException("Unable to load " + catalogImplementor);

        if (!result.connect(props))
            throw new RuntimeException(
                    "Unable to connect to replica catalog implementation "
                            + catalogImplementor
                            + " with props "
                            + props);

        // done
        return result;
    }

    public static ReplicaCatalog loadInstance(CommonProperties props) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
    }
}
