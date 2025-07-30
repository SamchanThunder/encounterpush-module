/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.encounterpush;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.BaseModuleActivator;

import org.openmrs.Encounter;
import org.openmrs.event.Event;

/**
 * This class contains the logic that is run every time this module is either started or shutdown
 */
public class EncounterPusherActivator extends BaseModuleActivator {
	
	private Log log = LogFactory.getLog(this.getClass());
	
	private EncounterPusher encounterListener;
	
	/**
	 * @see #started()
	 */
	public void started() {
		log.info("Started Encounter push module");
		
		encounterListener = new EncounterPusher();
		
		Event.subscribe(Encounter.class, Event.Action.CREATED.name(), encounterListener);
	}
	
	/**
	 * @see #shutdown()
	 */
	public void shutdown() {
		log.info("Shutdown Patient push module");
		
		if (encounterListener != null) {
			Event.unsubscribe(Encounter.class, Event.Action.CREATED, encounterListener);
		}
	}
	
}
