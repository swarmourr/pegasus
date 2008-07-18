/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.griphyn.cPlanner.namespace;

import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.common.PegasusProperties;


/**
 * The environment namespace, that puts in the environment variables for the
 * transformation that is being run, through Condor. At present on the occurence
 * of a clash between the values of an environment variable the values are
 * overwritten with the order of preference in decreasing order being users
 * local properties, transformation catalog, pool file and the dax (vdl).
 * Later on operations like append , prepend would also be supported.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */

public class ENV extends Namespace {

    /**
     * The name of the namespace that this class implements.
     */
    public static final String NAMESPACE_NAME = Profile.ENV;

    /**
     * The name of the environment variable that specifies the path to the
     * proxy.
     */
    public static final String X509_USER_PROXY_KEY = "X509_USER_PROXY";

    /**
     * The name of the environment variable that specifies the Gridstart PREJOB.
     */
    public static final String GRIDSTART_PREJOB = "GRIDSTART_PREJOB";

    /**
     * The name of the implementing namespace. It should be one of the valid
     * namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;

    /**
     * The default constructor.
     * Note that the map is not allocated memory at this stage. It is done so
     * in the overloaded construct function.
     */
    public ENV() {
        mProfileMap = null;
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp  map (possibly empty).
     */
    public ENV(Map mp) {
        mProfileMap = new TreeMap(mp);
        mNamespace = NAMESPACE_NAME;
    }

    

    /**
     * Returns the name of the namespace associated with the profile implementations.
     *
     * @return the namespace name.
     * @see #NAMESPACE_NAME
     */
    public String namespaceName(){
        return mNamespace;
    }



    /**
     * Constructs a new element of the format (key=value). It first checks if
     * the map has been initialised or not. If not then allocates memory first.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        if(mProfileMap == null)
            mProfileMap = new TreeMap();
        mProfileMap.put(key, value);
    }


    /**
     * This checks whether the key passed by the user is valid in the current
     * namespace or not. At present, for this namespace all the keys are
     * construed as valid as long as the value passed is not null.
     *
     * @param  key (left hand side)
     * @param  value (right hand side)
     *
     * @return Namespace.VALID_KEY
     * @return Namespace.NOT_PERMITTED_KEY
     *
     */
    public int checkKey(String key, String value) {
        if(key == null || value == null)
            return Namespace.NOT_PERMITTED_KEY;
        return Namespace.VALID_KEY;
    }

    /**
     * Converts the contents of the map into the string that can be put in the
     * Condor file for printing.
     */
    public String toString() {
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;

        Iterator it = (mProfileMap == null) ? null: mProfileMap.keySet().iterator();
        if(it == null)
            return null;

        st.append("environment = ");
        while(it.hasNext()){
            key = (String)it.next();
            value = (String)mProfileMap.get(key);
            st.append(key).append("=").append(value).append(";");
        }
        st.append("\n");
        return st.toString();

    }

    /**
    * It puts in the namespace specific information specified in the properties
    * file into the namespace. The name of the pool is also passed, as many of
    * the properties specified in the properties file are on a per pool basis.
    *
    * @param properties  the <code>PegasusProperties</code> object containing
    *                    all the properties that the user specified at various
    *                    places (like .chimerarc, properties file, command line).
    * @param pool        the pool name where the job is scheduled to run.
    */
   public void checkKeyInNS(PegasusProperties properties, String pool){
       //get from the properties for pool local
       String prop = pool.equalsIgnoreCase("local") ?
            //check if property in props file
            properties.getLocalPoolEnvVar() :
            null;

        if (prop != null) {
            checkKeyInNS(prop);
        }

   }


    /**
     * It takes in key=value pairs separated by a ; and puts them into the
     * namespace after checking if they are valid or not.
     *
     * @param envString   the String containing the environment variables and
     *                    their values separated by a semi colon.
     */
    public void checkKeyInNS(String envString){
        //sanity check
        if(envString == null)
            return;

        StringTokenizer st = new StringTokenizer(envString,";");
        String name;
        String value;
        String keyValPair;

        while(st.hasMoreTokens()){
            keyValPair = (String)st.nextToken(";");
            if(keyValPair.trim().equalsIgnoreCase("null")){
                return;
              }
              StringTokenizer st1 = new StringTokenizer(keyValPair);

              name = st1.nextToken("=");
              value= st1.nextToken();

              checkKeyInNS(name,value);
        }

    }

    /**
     * Merge the profiles in the namespace in a controlled manner.
     * In case of intersection, the new profile value overrides, the existing
     * profile value.
     *
     * @param profiles  the <code>Namespace</code> object containing the profiles.
     */
    public void merge( Namespace profiles ){
        //check if we are merging profiles of same type
        if (!(profiles instanceof ENV )){
            //throw an error
            throw new IllegalArgumentException( "Profiles mismatch while merging" );
        }
        String key;
        for ( Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ){
            //construct directly. bypassing the checks!
            key = (String)it.next();
            this.construct( key, (String)profiles.get( key ) );
        }
    }


    /**
     * Returns a copy of the current namespace object.
     *
     * @return the Cloned object
     */
    public Object clone() {
        return ( mProfileMap == null ? new ENV() : new ENV(this.mProfileMap) );
    }

}
