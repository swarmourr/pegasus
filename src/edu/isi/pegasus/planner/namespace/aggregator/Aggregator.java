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
package edu.isi.pegasus.planner.namespace.aggregator;

/**
 * An internal interface, that allows us to perform aggregation functions on profiles during merging
 * of profiles.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface Aggregator {

    /** The version number associated with this API of Profile Aggregators. */
    public static final String VERSION = "1.0";

    /**
     * Do the aggregation function on the profile values.
     *
     * @param oldValue the existing value for the profile.
     * @param newValue the new value being added to the profile.
     * @param dflt the default value to be used in case the values are not of the correct type.
     * @return the computed value as a String.
     */
    public String compute(String oldValue, String newValue, String dflt);
}
