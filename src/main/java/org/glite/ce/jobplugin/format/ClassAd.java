/*
 * Copyright (c) Members of the EGEE Collaboration. 2004. 
 * See http://www.eu-egee.org/partners/ for details on the copyright
 * holders.  
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
 
/*
 *
 * Authors: Luigi Zangrando <zangrando@pd.infn.it>
 *
 */

package org.glite.ce.jobplugin.format;

import java.util.Hashtable;

import org.glite.ce.monitorapij.sensor.SensorException;
import org.glite.ce.monitorapij.sensor.SensorOutputDataFormat;

public class ClassAd extends SensorOutputDataFormat {

   public ClassAd() {
      super("CLASSAD");
      
      //Sets the supported query languages
      setSupportedQueryLang(new String[] { "ClassAd" });
   }



   public String[] apply(Hashtable parameters) throws Exception {
      if(parameters == null) {
         throw (new Exception("CLASSAD apply error: parameter is null"));     
      }

      String[] jobStatus = (String[]) parameters.get("jobStatus");

      if(jobStatus != null) {
        /* String[] msg = new String[jobStatus.length];
         for(int i=0; i<jobStatus.length; i++) {
             msg[i] = "[" + jobStatus[i] + "]";
         }

         return msg;*/
         return jobStatus;
      } else {
         throw (new SensorException("CLASSAD apply error: jobStatus list is null"));
      }
   }

}
