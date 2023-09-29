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
package edu.isi.pegasus.planner.selector.transformation;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.selector.TransformationSelector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This implementation of the Selector select a transformation of type STAGEABLE on all sites.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Staged extends TransformationSelector {

    /**
     * Takes a list of TransformationCatalogEntry objects and returns 1 or many
     * TransformationCatalogEntry objects as a list by selecting only Static stageable binary's
     *
     * @param tcentries List
     * @param preferredSite the preferred site for selecting the TC entries
     * @return List
     */
    public List getTCEntry(List<TransformationCatalogEntry> tcentries, String preferredSite) {
        List results = null;
        for (Iterator i = tcentries.iterator(); i.hasNext(); ) {
            TransformationCatalogEntry tc = (TransformationCatalogEntry) i.next();
            if (tc.getType().equals(TCType.STAGEABLE)) {
                if (results == null) {
                    results = new ArrayList(5);
                }
                results.add(tc);
            }
        }
        return results;
    }
}
