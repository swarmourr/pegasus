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

package edu.isi.pegasus.planner.catalog.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FileDetector;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * A factory class to load the appropriate implementation of Site Catalog as specified by
 * properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteFactory {

    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.catalog.site.impl";

    /** For 4.2, the orginal XML3 class was renamed XML and it supports different schemas. */
    private static final String XML_IMPLEMENTING_CLASS_BASENAME = "XML";

    private static final String YAML_IMPLEMENTING_CLASS_BASENAME = "YAML";

    public static final String DEFAULT_SITE_CATALOG_IMPLEMENTOR = "YAML";

    /** The default basename of the yaml site catalog file. */
    public static final String DEFAULT_YAML_SITE_CATALOG_BASENAME = "sites.yml";

    /** The default basename of the site catalog file. */
    public static final String DEFAULT_XML_SITE_CATALOG_BASENAME = "sites.xml";

    /**
     * @param sites list of sites
     * @param bag the bag of pegasus objects
     * @return SiteStore object containing the information about the sites.
     */
    public static SiteStore loadSiteStore(Collection<String> sites, PegasusBag bag) {
        LogManager logger = bag.getLogger();
        SiteStore result = new SiteStore();
        if (sites.isEmpty()) {
            logger.log(
                    "No sites given by user. Will use sites from the site catalog",
                    LogManager.DEBUG_MESSAGE_LEVEL);
            sites.add("*");
        }
        SiteCatalog catalog = null;

        /* load the catalog using the factory */
        catalog = SiteFactory.loadInstance(bag);

        /* always load local site */
        List<String> toLoad = new ArrayList<String>(sites);
        toLoad.add("local");

        /* load the sites in site catalog */
        try {
            catalog.load(toLoad);

            /* query for the sites, and print them out */
            logger.log("Sites loaded are " + catalog.list(), LogManager.DEBUG_MESSAGE_LEVEL);

            // load into SiteStore from the catalog.
            for (Iterator<String> it = toLoad.iterator(); it.hasNext(); ) {
                SiteCatalogEntry s = catalog.lookup(it.next());
                if (s != null) {
                    result.addEntry(s);
                }
            }
        } catch (SiteCatalogException e) {
            throw new RuntimeException("Unable to load from site catalog ", e);
        } finally {
            /* close the connection */
            try {
                catalog.close();
            } catch (Exception e) {
            }
        }

        return result;
    }

    /**
     * Connects the interface with the site catalog implementation.
     *
     * @param bag bag of Pegasus initialization objects
     * @return handle to the Site Catalog.
     * @throws SiteFactoryException that nests any error that might occur during the instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static SiteCatalog loadInstance(PegasusBag bag) throws SiteFactoryException {

        PegasusProperties properties = bag.getPegasusProperties();
        if (properties == null) {
            throw new SiteFactoryException("Invalid NULL properties passed");
        }
        File dir = bag.getPlannerDirectory();
        if (dir == null) {
            throw new SiteFactoryException("Invalid Directory passed");
        }
        if (bag.getLogger() == null) {
            throw new SiteFactoryException("Invalid Logger passed");
        }
        Properties connect = properties.matchingSubset(SiteCatalog.c_prefix, false);
        /* get the implementor from properties */
        String catalogImplementor = properties.getSiteCatalogImplementor();
        String endpoint = null;
        if (catalogImplementor == null) {
            // PM-1448 check if pegasus.catalog.site.file property is specified
            endpoint = properties.getProperty(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY);
            if (endpoint == null) {
                // PM-1486 check for default files
                File defaultYAML = new File(dir, SiteFactory.DEFAULT_YAML_SITE_CATALOG_BASENAME);
                File defaultXML = new File(dir, SiteFactory.DEFAULT_XML_SITE_CATALOG_BASENAME);
                if (exists(defaultYAML)) {
                    catalogImplementor = SiteFactory.YAML_IMPLEMENTING_CLASS_BASENAME;
                    connect.setProperty("file", defaultYAML.getAbsolutePath());
                } else if (exists(defaultXML)) {
                    catalogImplementor = SiteFactory.XML_IMPLEMENTING_CLASS_BASENAME;
                    connect.setProperty("file", defaultXML.getAbsolutePath());
                } else {
                    // then just set to default implementor and let the implementing class load
                    catalogImplementor = SiteFactory.DEFAULT_SITE_CATALOG_IMPLEMENTOR;
                }
            } else {
                // detect type based on contents
                catalogImplementor = detectType(endpoint);
            }
        }

        /* prepend the package name if required */
        catalogImplementor =
                (catalogImplementor.indexOf('.') == -1)
                        ?
                        // pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + catalogImplementor
                        :
                        // load directly
                        catalogImplementor;

        // determine the class that implements the site catalog
        return loadInstance(catalogImplementor, bag, connect);
    }

    /**
     * Connects the interface with the site catalog implementation. The choice of backend is
     * configured through properties.
     *
     * @param catalogImplementor the name of the class implementing catalog
     * @param bag bag of initalization objects
     * @param properties the connection properties.
     * @return handle to the Site Catalog.
     * @throws SiteFactoryException that nests any error that might occur during the instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    private static SiteCatalog loadInstance(
            String catalogImplementor, PegasusBag bag, Properties properties) {
        if (properties == null) {
            throw new SiteFactoryException("Invalid NULL properties passed");
        }

        SiteCatalog catalog = null;
        try {
            if (catalogImplementor == null) {
                throw new RuntimeException(
                        "You need to specify the " + SiteCatalog.c_prefix + " property");
            }

            /* prepend the package name if required */
            catalogImplementor =
                    (catalogImplementor.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + catalogImplementor
                            :
                            // load directly
                            catalogImplementor;

            DynamicLoader dl = new DynamicLoader(catalogImplementor);
            catalog = (SiteCatalog) dl.instantiate(new Object[0]);

            if (catalog == null) {
                throw new RuntimeException("Unable to load " + catalogImplementor);
            }
            catalog.initialize(bag);

            if (!catalog.connect(properties))
                throw new RuntimeException("Unable to connect to site catalog implementation");
        } catch (Exception e) {
            throw new SiteFactoryException(
                    "Unable to instantiate Site Catalog ", catalogImplementor, e);
        }

        return catalog;
    }

    /**
     * Detect the type of endpoint usually a file
     *
     * @param endpoint
     * @return
     */
    private static String detectType(String endpoint) {
        String implementor = null;
        File file = new File(endpoint);
        if (file.exists() && file.canRead()) {
            // detect type of file
            implementor =
                    FileDetector.isTypeXML(file)
                            ? XML_IMPLEMENTING_CLASS_BASENAME
                            : YAML_IMPLEMENTING_CLASS_BASENAME;
        } else {
            throw new SiteFactoryException("Unable to read file" + file);
        }
        return implementor;
    }

    /**
     * Returns whether a file exists or not
     *
     * @param file
     * @return
     */
    private static boolean exists(File file) {
        return file == null ? false : file.exists() && file.canRead();
    }
}
