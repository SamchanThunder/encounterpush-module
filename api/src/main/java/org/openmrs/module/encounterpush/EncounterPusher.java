package org.openmrs.module.encounterpush;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Scanner;
import javax.jms.Message;
import javax.jms.MapMessage;
import java.util.Map;
import java.util.List;
import org.openmrs.event.Event;
import org.openmrs.event.EventListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EncounterPusher implements EventListener {
	
	private Log log = LogFactory.getLog(this.getClass());
	
	// Name of client system
	private String systemName = "openmrs";
	
	// Servers (Respective to my Local Environment)
	private String urlMRS = "http://openmrs:8080/openmrs/ws/fhir2/R4/Encounter/";
	
	private String urlHIM = "http://openhim-core:5001/fhir/Encounter";
	
	private String urlHIMPatient = "http://openhim-core:5001/fhir/Patient/";
	
	// OpenMRS Auth Credentials (Must be hidden in production)
	private String usernameMRS = "admin";
	
	private String passwordMRS = "Admin123";
	
	// OpenHIM Auth Credentials (Must be hidden in production)
	private String usernameHIM = "interop-client";
	
	private String passwordHIM = "interop-password";
	
	@Override
	public void onMessage(Message message) {
        String uuid = null;
		try {
            MapMessage mapMessage = (MapMessage) message; // Changes message to Key Value pairs.
            String action = mapMessage.getString("action"); // Checks the value of the action key. What event happened (CREATE, UPDATE, etc)?
            
            // Checks if action was a create. Future Consideration: Implement update patient
            // The Activator file only allows CREATED actions to here, but this is a double check.
            if (Event.Action.CREATED.toString().equals(action)) {
                log.info("Encounter push: ENCOUNTER CREATED");
                uuid = mapMessage.getString("uuid"); // Gets value of uuid key

                // Fetch Encounter Data from OpenMRS
                URL getUrl = new URL(urlMRS + uuid);
                log.info("Encounter push: GOT URL: " + urlMRS + uuid);

                HttpURLConnection getCon = (HttpURLConnection) getUrl.openConnection(); // Opens connection to URL
                log.info("Encounter push: OPEN URL SUCCESSFUL");

                getCon.setRequestMethod("GET");

                String authMRS = usernameMRS + ":" + passwordMRS;
                String encodedAuthMRS = Base64.getEncoder().encodeToString(authMRS.getBytes(StandardCharsets.UTF_8)); // Required format for auth
                getCon.setRequestProperty("Authorization", "Basic " + encodedAuthMRS);
                getCon.setRequestProperty("Accept", "application/json"); // Make server return Json format

                String encounterJson = new Scanner(getCon.getInputStream(), "UTF-8").useDelimiter("\\A").next(); // Converts json to string

                log.info("Fetched encounter JSON: " + encounterJson);

                /*Add an identifier array to encounter json, then make an element which contains 
                use: "secondary", system: systemName, value: source uuid of subject reference (patient)
                */
                ObjectMapper mapper = new ObjectMapper();

                Map<String, Object> jsonMap = mapper.readValue(encounterJson, Map.class);

                List<Map<String, Object>> identifiers = (List<Map<String, Object>>) jsonMap.get("identifier");

                // The encounter data should originally have no identifiers, so this should not happen.
                if (identifiers == null) {
                    return;
                }

                // Get subject ref uid from encounter data
                Map<String, Object> subject = (Map<String, Object>) jsonMap.get("subject");
                String subjectRef = (String) subject.get("reference");
                String openmrsPatientId = subjectRef.substring("Patient/".length());

                Map<String, Object> newIdentifier = new HashMap<>();
                newIdentifier.put("use", "secondary");
                newIdentifier.put("system", systemName);
                newIdentifier.put("value", openmrsPatientId);

                identifiers.add(newIdentifier);
            
                /* Essential for OpenHIM to accept with current logic. This is considered bad practice.
                If you want to fix this, then you must alter the location and participant (practitioner) on this encounter data
                to match the right location and participant uid's on OpenHIM FHIR Server. 
                Although we remove these attributes, these attributes are still present in the encounter data but as a string
                in the "text" attribute, so these values are not completely gone and can still be accessed.
                */
                jsonMap.remove("partOf");
                jsonMap.remove("location");
                jsonMap.remove("participant");

                // Find correct subject reference UID on OpenHIM
                String searchPatientUrlStr = urlHIMPatient + "?identifier=" + java.net.URLEncoder.encode(systemName + "|" + openmrsPatientId, "UTF-8");
                URL searchPatientUrl = new URL(searchPatientUrlStr);
                HttpURLConnection searchCon = (HttpURLConnection) searchPatientUrl.openConnection();
                searchCon.setRequestMethod("GET");
                String authHIM = usernameHIM + ":" + passwordHIM;
                String encodedAuthHIM = Base64.getEncoder().encodeToString(authHIM.getBytes(StandardCharsets.UTF_8));
                searchCon.setRequestProperty("Authorization", "Basic " + encodedAuthHIM);
                searchCon.setRequestProperty("Accept", "application/json");

                String searchResponseJson = new Scanner(searchCon.getInputStream(), "UTF-8").useDelimiter("\\A").next();

                Map<String, Object> searchResponseMap = mapper.readValue(searchResponseJson, Map.class);

                /* The response body is a Bundle of Patient resources in "entry." We get the first element in entries as
                each entry should only have one entry in an ideal environment because no two systems should have the same
                system name.
                */
                List<Map<String, Object>> entries = (List<Map<String, Object>>) searchResponseMap.get("entry");
                if (entries != null && !entries.isEmpty()) {
                    Map<String, Object> firstEntry = entries.get(0);
                    Map<String, Object> patientResource = (Map<String, Object>) firstEntry.get("resource");
                    String openhimPatientId = (String) patientResource.get("id");
                    log.info("Found OpenHIM patient with id: " + openhimPatientId);

                    // Update the encounter subject reference to use openhimPatientId
                    subject.put("reference", "Patient/" + openhimPatientId);

                    // Make json to string for posting
                    encounterJson = mapper.writeValueAsString(jsonMap);
                }else{
                    return;
                }

                // Post Data to OpenHIM FHIR Server
                URL postUrl = new URL(urlHIM);
                HttpURLConnection postCon = (HttpURLConnection) postUrl.openConnection(); // Opens connection to URRL
                postCon.setRequestMethod("POST");
                postCon.setDoOutput(true); // We want to write data with a request body
                postCon.setRequestProperty("Content-Type", "application/json");

                postCon.setRequestProperty("Authorization", "Basic " + encodedAuthHIM);

                // Posts encounterJson
                try (OutputStream os = postCon.getOutputStream()) {
                    byte[] input = encounterJson.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }

                int responseCode = postCon.getResponseCode();

                getCon.disconnect();
                postCon.disconnect();
            }
        }
		catch (Exception e) {
			 log.error("Failed to push encounter to FHIR server. UUID: " + uuid, e);
		}
	}
}
