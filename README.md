# <ins>Encounter Pusher Module</ins>
### OpenMRS Module that listens for encounter creation/registration, alters the subject reference uid to match the correct patient on OpenHIM, then posts it to an OpenHIM FHIR server in json format.
-------------

## **Module (.omod) Location**
```
./encounterpush/omod/target/encounterpush-1.0.0.omod
```

## **Required Modules on OpenMRS**
```
Event Module (https://addons.openmrs.org/show/org.openmrs.module.event)
OpenMRS FHIR2 Module (https://addons.openmrs.org/show/org.openmrs.module.openmrs-fhir2-module)
```

## **Editing Module**

Edit these files to make changes:
```
PatientPusher.java
  i.  Where URL connection, authentication, fetching, correcting subject reference uid, and posting happens.
  ii. If you wanted to change the format of the encounter data posted, here is where you would do it.        
PatientPusherActivator.java
  i.  Start/Shutdown Module logic.
  ii. Subscribes to ecounter creation notifications here on start here.
patientpush/pom.xml
  i.  Change the name, description, and version of module shown on OpenMRS
```

Rebuild Module (Linux)
```
sudo apt install maven   # (One Time Command)

mvn clean install
```

Relevant Folder Structure
```
|--patientpush
|  |--api
|    |--main/java/org/openmrs/module/encounterpush
|      |--EncounterPusher.java
|      |--EncounterPusherActivator.java
|  |--omod
|     |--target
|        |--encounterpush-1.0.0.omod
|  |--pom.xml
|--README.md
```