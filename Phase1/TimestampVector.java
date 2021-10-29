/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();
	
	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		// Si el timestamp no es nulo
		if( timestamp != null ) {
			// Capturamos el host del timestamp pasado por par�metro.
			String host = timestamp.getHostid();
			// Lo guardamos en el hashmap. 
			// Si la clave existe se sobreescribir� el timestamp.
			this.timestampVector.put(host, timestamp);
		}
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
		// Recorremos los host de nuestro hashmap.
		for(String host : this.timestampVector.keySet()) {
			// Capturamos el timestamp asociado al host local
			Timestamp local_ts = this.timestampVector.get(host);
			// Capturamos el timestamp asociado al par�metro
			Timestamp remote_ts = tsVector.timestampVector.get(host);
			// Si el local es m�s antiguo que el remoto, actualizamos el local.
			if( local_ts.compare(remote_ts) < 0 ) this.timestampVector.put(host, remote_ts);
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public synchronized Timestamp getLast(String node){
		return this.timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		// Recorremos el vector pasado opr par�metro.
		for( String host : tsVector.timestampVector.keySet() ) {
			// Capturamos el timestamp del host local.
			Timestamp local_ts = this.timestampVector.get(host);
			// Capturamos el timestamp del host del par�metro.
			Timestamp remote_ts = tsVector.timestampVector.get(host);
			// Si el par�metro es m�s peque�o que el local
			if( remote_ts.compare(local_ts) < 0 ) {
				// Sobreescribimos el local.
				this.timestampVector.put(host, remote_ts); 
			}
		}
		
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampVector clone(){
		// Declaraci�n de los host
		List<String> hosts = new ArrayList();;
		// Llenamos la lista con los hosts.
		for( String host : this.timestampVector.keySet() ) 	hosts.add(host);
		// Instanciamos un objeto nuevo.
		TimestampVector new_tsv = new TimestampVector(hosts);
		// Recorremos el vector local y asignamos los datos a el nuevo objeto.
		for( String host : this.timestampVector.keySet()  ) {
			Timestamp ts = this.timestampVector.get(host);
			new_tsv.timestampVector.put(host, ts);
		}
		return new_tsv;
	}
	
	/**
	 * equals
	 */
	public synchronized boolean equals(Object obj){
		// Si el objeto par�metro es nullo
		if( obj == null ) return false;
		// Si el objeto par�metro no es intancia de TimestampVector
		if ( !(obj instanceof TimestampVector) ) return false;
		// Hacemos el casting
		TimestampVector remote_ts = (TimestampVector) obj;
		// Invocamos el m�todo equals. 
		return this.timestampVector.equals(remote_ts.timestampVector);
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
			LSimLogger.log(Level.TRACE, "valor de all: "+ all);
		}
		return all;
	}
}
