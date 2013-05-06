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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.glite.ce.creamapi.jobmanagement.Job;
import org.glite.ce.creamapi.jobmanagement.JobStatus;
import org.glite.ce.jobplugin.format.ClassAd;
import org.glite.ce.monitorapij.resource.types.Property;
import org.glite.ce.monitorapij.sensor.AbstractSensor;
import org.glite.ce.monitorapij.sensor.Sensor;
import org.glite.ce.monitorapij.sensor.SensorException;

public final class CREAMJobSensor extends AbstractSensor {
    private static final long serialVersionUID = 1L;

    private final static Logger logger = Logger.getLogger(CREAMJobSensor.class.getName());
    
    private int expiration;

    private ServerSocket serverSocket = null;
    private static final int SERVER_SOCKET_TIMEOUT = 60000; //ms.

    private boolean working = true;

    private static final int THREAD_COUNT = 20;

    //private final ThreadPoolExecutor pool = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 10, TimeUnit.SECONDS, new LinkedBlockingQueue());
    private ThreadPoolExecutor pool = null;
    
    /** Creates a new instance of CEJobSensor */
    public CREAMJobSensor() {
        super("CREAM Job Sensor", "CREAM_JOBS");

        Property[] properties = new Property[] { 
                new Property("executionDelay", "60000"),
                new Property("pushMode", "false"),
                new Property("expiration", "60")
        };

        setProperty(properties);
        setEventOverwriteModeActive(true);
        setScope(Sensor.LOW);

        working = false;

        // makes an instance of the outputFormats supported by the sensor
        ClassAd defaultFormat = new ClassAd();

        addFormat(defaultFormat);
        setDefaultFormat(defaultFormat);
    }

    public void init() throws SensorException {
        super.init();

        Property listenerPort = getProperty("LISTENER_PORT");
        int port = -1;

        if (listenerPort != null) {
            try {
                port = Integer.parseInt(listenerPort.getValue());
            } catch (Throwable e) {
                //logger.warn("Found wrong value for the LISTENER_PORT parameter \"" + listenerPort + "\": using the default: " + port);
                logger.error("Found wrong value for the LISTENER_PORT parameter " + listenerPort);
                //pool.shutdownNow();
                throw new SensorException("Found wrong value for the LISTENER_PORT parameter " + listenerPort);
            }
        } else {
            logger.error("LISTENER_PORT parameter not found.");
            //pool.shutdownNow();
            throw new SensorException("LISTENER_PORT parameter not found.");
        }

        try {
            serverSocket = new ServerSocket(port, 10);
            serverSocket.setSoTimeout(SERVER_SOCKET_TIMEOUT);
            logger.debug("Server socket timeout is set to: " + SERVER_SOCKET_TIMEOUT + " ms.");
            pool = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 10, TimeUnit.SECONDS, new LinkedBlockingQueue());
            working = true;
            logger.debug("Server listening on port " + port);
        } catch (IOException e) {
            logger.error("Problem to create the ServerSocket: " + e.getMessage(), e);
            //pool.shutdownNow();
        }
        
        Property expirationProperty = getProperty("expiration");
        try {
            if (expirationProperty != null && expirationProperty.getValue() != null) {
                expiration = Integer.parseInt(expirationProperty.getValue());
            }
        } catch (NumberFormatException numEx) {
            logger.error("Found wrong value (" + expiration + ") for the expiration parameter: " + numEx.getMessage());
        }
    }

    public void startSensor() {
        super.startSensor();
        logger.debug("startSensor executed!");
    }

    public void suspendSensor() {
        super.suspendSensor();
        working = false;
        logger.debug("suspendSensor executed!");
    }

    public void resumeSensor() {
        super.resumeSensor();
        working = true;
        startSensor();
        logger.debug("resumeSensor executed!");
    }

    public void destroySensor() {
    	super.destroySensor();
        logger.debug("Destroying listeners");
        working = false;
    }

    public void execute() throws SensorException {
        logger.debug("execute BEGIN");
        if (serverSocket == null) {
            try {
                init();
                logger.debug("init completed");
            } catch (SensorException e) {
                logger.error("init error: " + e.getMessage(), e);
                return;
            }
        }
        Socket socket = null;
        while (working) {
            try {
                socket = serverSocket.accept();
                pool.execute(new Worker(socket));
            } catch (SocketTimeoutException ste) {
          	  //logger.debug(ste.getMessage());
            } catch (IOException e) {
              logger.error(e.getMessage(), e);
            }
        }
        try {
            pool.shutdownNow();
            serverSocket.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        logger.debug("execute END");

    }

    private void fireJobChanged(Job job) {
        if (job == null) {
            return;
        }

        String userId = job.getUserId();
        logger.debug("fireJobChanged with userId " + userId);


        if (userId == null || userId.length() == 0) {
            logger.error("userId is invalid: job " + job.getId());
            return;
        }

        List<JobStatus> status = job.getStatusHistory();
        if (status == null || status.size() == 0) {
            return;
        }

        ArrayList classadList = new ArrayList(0);

        for (int i = 0; i < status.size(); i++) {
            JobStatus tmpStat = status.get(i);

            if (tmpStat != null && (tmpStat.getExitCode() == null || !tmpStat.getExitCode().equals("W"))) {
                StringBuffer buff = new StringBuffer("[\n\tCREAM_JOB_ID = \"");
//                String jobId = job.getCreamURL();
//                if(jobId != null) {
//                    int index = jobId.indexOf("ce-cream");
//                    if(index > 0 ) {
//                        jobId = jobId.substring(0, index);
//                    }
//                }
//                
//                jobId += job.getId();
//                
//                buff.append(jobId).append("\";\n");
                buff.append(job.getId()).append("\";\n");
                buff.append("\tCREAM_URL = \"").append(job.getCreamURL()).append("\";\n");
                buff.append("\tJOB_STATUS = \"").append(tmpStat.getName()).append("\";\n");
                buff.append("\tTIMESTAMP = \"").append(tmpStat.getTimestamp().getTimeInMillis()).append("\"");

                if (tmpStat.getExitCode() != null) {
                    buff.append(";\n\tEXIT_CODE = ").append(tmpStat.getExitCode());
                }

                if (job.getICEId() != null) {
                    buff.append(";\n\tICE_ID = \"").append(job.getICEId()).append("\"");
                }

                if (job.getWorkerNode() != null) {
                    buff.append(";\n\tWORKER_NODE = \"").append(job.getWorkerNode()).append("\"");
                }

                if (tmpStat.getFailureReason() != null) {
                    buff.append(";\n\tFAILURE_REASON = \"").append(tmpStat.getFailureReason()).append("\"");
                }

                if (tmpStat.getDescription() != null) {
                    buff.append(";\n\tDESCRIPTION = \"").append(tmpStat.getDescription()).append("\"");
                }

                buff.append("\n]");
                classadList.add(buff.toString());
            }
        }

        String[] classads = new String[classadList.size()];
        classads = (String[]) classadList.toArray(classads);

        Calendar startTime = Calendar.getInstance();
        Calendar endTime = ((Calendar) startTime.clone());

        if (job.getLastStatus().getType() == JobStatus.PURGED) {
            endTime.add(Calendar.MINUTE, 5); // 5 minutes
        } else {
            endTime.add(Calendar.MINUTE, expiration);
        }
        
        logger.debug("JOB_SENSOR received event: job = " + job.getId() + "  status = " + job.getLastStatus().getName());
        fireSensorEvent(new CREAMJobSensorEvent(this, startTime, endTime, job.getId(), userId, job.getVirtualOrganization(), classads));
    }

    class Worker implements Runnable {
        private Socket clientSocket;

        public Worker(Socket s) {
            clientSocket = s;
            try {
                clientSocket.setKeepAlive(false);
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        public void run() {
            ObjectInputStream ois = null;
            try {
                if (clientSocket != null) {
                    ois = new ObjectInputStream(clientSocket.getInputStream());
                    Job job = null;
                    while (true) {
                        job = new Job();
                        job.readExternal(ois);
                        logger.debug("executed a job.readExternal()");
                        fireJobChanged(job);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                try {
                    if (ois != null) {
                        ois.close();
                        ois = null;
                    }
                    if (clientSocket != null) {
                        clientSocket.close();
                        clientSocket = null;
                    }
                } catch (IOException ioe) {
                    logger.error(ioe.getMessage());
                }
            }            
        }
    }
}
