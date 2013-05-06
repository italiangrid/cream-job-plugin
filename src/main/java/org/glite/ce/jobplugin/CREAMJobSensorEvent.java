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

package org.glite.ce.jobplugin;

import java.util.Calendar;

import org.glite.ce.monitorapij.sensor.Sensor;
import org.glite.ce.monitorapij.sensor.SensorEvent;
import org.glite.ce.monitorapij.sensor.SensorException;

public class CREAMJobSensorEvent extends SensorEvent {
    private static final long serialVersionUID = 1L;

    public CREAMJobSensorEvent(Sensor source, Calendar creationTime, Calendar expirationTime, String eventId, String receiverId, String receiverGroup, String[] jobStatus){
        super(source, -1, creationTime, expirationTime);
        this.setName(eventId);
        this.setReceiverId(receiverId);
        this.setReceiverGroup(receiverGroup);
     //   this.setMessage(classads);
        addParameter("jobStatus", jobStatus); 
        
        try {
           applyFormat(source.getDefaultFormat());
        } catch (SensorException e) {
           e.printStackTrace();
        }
    }

}
