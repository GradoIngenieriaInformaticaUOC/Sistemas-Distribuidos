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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel LÃ¡zaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.u
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op){
		// Caoturamos el timestamp y el host_id de la operación.
		Timestamp t = op.getTimestamp();
		String h_id = t.getHostid();
		// Capturamos la lista asociada a ese host en nuestro principal
		List<Operation> op_list = this.log.get(h_id);
		// Creamos un timestamp inicialmente nulo.
		Timestamp top_ts = null;
		// Si hay alguna operación guardada en nuestra lista.
		if(!op_list.isEmpty() || op_list != null) {
			// Capturamos el último elemento, el más reciente.
			top_ts = op_list.get(op_list.size()-1).getTimestamp();
		}
		// Comparamos ambas operaciones.
		long compare_result = t.compare(top_ts);
		// Si la operación a insertar es más reciente que la que tenemos
		if((compare_result > 0 && top_ts != null) || (compare_result == 0 && top_ts == null)) {
			// Añadimos la operación a la lista
			this.log.get(h_id).add(op);
			// Se ha insertado correctamente
			return true;
		}
		// No se ha insertado
		return false;
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum){
		
		// Cremos una lista de operaciones vacía.
		List<Operation> list = new Vector<Operation>();
		
		// Recorremos los host de nuestro log.
		for( String host : this.log.keySet() ) {
			// Capturamos las operaciones de el host que estamos evaluando.
			List<Operation> ops_host = this.log.get(host);
			// Capturamos el último timestamp del host que evaluamos del array por parámetro.
			Timestamp last_ts = sum.getLast(host);
			// Por cada operación del host en nuestro log.
			for( Operation op : ops_host ) {
				// Si la oepración a evaluar es 'más nueva' que la del parámetro la añadimos
				// a la lista.
				if( op.getTimestamp().compare(last_ts) > 0 ) list.add(op);
			}
		}
		// Devolvemos la lista.
		return list;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		// Si es nulo devolvemos false.
		if( obj == null ) return false;
		// Si no es una instacia de Log devolvemos false.
		if( !(obj instanceof Log) ) return false;
		
		// Capturamos el log y hacemos el casting.
		Log log_to_compare = (Log) obj;
		
		// Devolvemos el resulttado.
		return this.log.equals(log_to_compare);
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}
