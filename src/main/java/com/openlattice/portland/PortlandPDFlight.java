package com.openlattice.portland;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class PortlandPDFlight {

    private static final Logger logger = LoggerFactory.getLogger( PortlandPDFlight.class );

    //which environment to send integration to
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    //Parse dates correctly, from input string columns. Offset from UTC.
    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_NewYork,
            "yyyy-MM-dd" );

    public static void main( String[] args ) throws InterruptedException, JsonProcessingException {
/*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */

        // final SparkSession sparkSession = MissionControl.getSparkSession();

        //gets csv path, username, pwd, put them in build.gradle run.args. These are the positions in the string arg.
        final String arpath = args[ 0 ];
        final String jwtToken = args[ 1 ];    // Get jwtToken to verify data integrator has write permissions to dataset using username/pwd above.

        SimplePayload arPayload = new SimplePayload( arpath );

        Map<Flight, Payload> flights = new LinkedHashMap<>( 2 );
        Flight arFlight = Flight.newFlight()
                .createEntities()

                .addEntity( "PortlandPDArrestee" )  //variable name within flight. Doesn't have to match anything anywhere else
                    .to( "PortlandPDArrestee" )       //name of entity set belonging to
                    .addProperty( "nc.PersonGivenName", "Arrestee First Name" )           //arrest report number
                    .addProperty( "nc.PersonMiddleName", "Arrestee Middle Name" )
                    .addProperty( "nc.PersonSurName", "Arrestee Last Name" )
                    .addProperty( "nc.PersonRace" ).value( PortlandPDFlight::standardRace ).ok()
                    .addProperty( "nc.PersonEthnicity" ).value( PortlandPDFlight::standardEthnicity ).ok()
                    .addProperty( "nc.PersonSex" ).value( PortlandPDFlight::standardSex ).ok()
                    .addProperty( "nc.PersonBirthDate" )
                    .value( row -> dtHelper.parseDate( row
                            .getAs( "Arrestee DOB Date" ) ) )   //these rows are shorthand for a full function .value as below
                    .ok()
                .endEntity()

                .addEntity( "PortlandPDIncident" )
                    .to( "PortlandPDIncident" )
                    .addProperty( "criminaljustice.incidentid", "Arrest Incident Number" )    //unique ID for address
                .endEntity()

                .addEntity( "PortlandPDOfficerPerson" )
                    .to("PortlandPDOfficerPerson")
                    .addProperty( "nc.PersonGivenName", "Arrresting Ofc Frist Name" )    //unique ID for address
                    .addProperty( "nc.PersonSurName", "Arresting Ofc Last Name" )    //unique ID for address
                    .addProperty( "nc.SubjectIdentification" , "Arresting Officer #" )
                .endEntity()

                .addEntity( "PortlandPDOfficer" )
                    .to("PortlandPDOfficer")
                    .addProperty( "publicsafety.officerid", "Arresting Officer #" )    //unique ID for address
                 .endEntity()

                .endEntities()

                .createAssociations()
                .addAssociation( "PortlandPDArrestedIn" )
                .to( "PortlandPDArrestedIn" )
                .fromEntity( "PortlandPDArrestee" )
                .toEntity( "PortlandPDIncident" )
                .entityIdGenerator(row -> UUID.randomUUID().toString())
//                    .addProperty( "ol.arrestdate" )
//                    .value( row -> dtHelper.parseDate( row
//                            .getAs( "Arrest Date" ) ) )   //these rows are shorthand for a full function .value as below
//                    .ok()
                    .addProperty( "location.address" ).value( PortlandPDFlight::standardAddress ).ok()
                    .addProperty( "ol.arrestcategory", "ARREST TYPE (186) eng" )
                    .addProperty( "criminaljustice.casenumber", "Arrest Charges Case #" )
                    .addProperty( "criminaljustice.nibrs", "UCR OFFN CODE (47) eng" )
                    .addProperty( "ol.numberofcounts", "Adult Arrest Charge Seq #" )
                    .addProperty( "ol.mapreference", "Map Ref# of Arst" )
                    .addProperty( "person.ageatevent").value( row -> Parsers.parseInt( row.getAs("Arrestee Age")) ).ok()
                    .addProperty( "j.ArrestCharge", "ARREST STATE CHARGE (7) eng" )
                .endAssociation()

                .addAssociation( "PortlandPDArrestedBy" )
                .to("PortlandPDArrestedBy")
                .fromEntity("PortlandPDArrestee")
                .toEntity( "PortlandPDOfficer" )
                .addProperty( "general.StringID", "Arrest Incident Number" )
                .endAssociation()

                .addAssociation( "PortlandPDBecomes" )
                .to("PortlandPDBecomes")
                .fromEntity( "PortlandPDOfficerPerson" )
                .toEntity( "PortlandPDOfficer" )
                .addProperty( "nc.SubjectIdentification", "Arrest Incident Number" )
                .endAssociation()

                .endAssociations()
                .done();

        ObjectMapper mapper = ObjectMappers.getYamlMapper();

//        logger.info("This is the JSON for the flight. {}", mapper.writeValueAsString(arFlight));
        // add all the flights to flights
        flights.put( arFlight, arPayload );
//
        // Send your flight plan to Shuttle and complete integration
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launchPayloadFlight( flights );

        //return flights;
    }

    // Custom Functions for Parsing CAD number from casenum - modified from functions for splitting first and last names

    public static String standardRace( Row row ) {
        String sr = row.getAs( "ARRESTEE RACE (11) eng" );
        String race = "";
        if ( sr != null ) {
            if ( sr.equals( "ASIAN OR PACIFIC ISLANDER" ) ) {
                race = "asian/pacisland";
            } else if ( sr.equals( "BLACK" ) ) {
                race = "black";
            } else if ( sr.equals( "UNKNOWN" ) | sr.equals( "HISPANIC" ) | sr.equals( "AMERICAN INDIAN / ALASKAN NATIVE" ) | sr.equals( "* Race *" )) {
                race = "";
            } else if ( sr.equals( "WHITE" ) ) {
                race = "white";
            }
        }
        return race;
    }

    public static String standardEthnicity( Row row ) {
        String sr = row.getAs( "ARRESTEE ETHNIC (32) eng" );

        if ( sr != null ) {
            if ( sr.equals( "HISPANIC" ) ) {
                return "hispanic";
            } else if ( sr.equals( "NONHISPANIC" ) ) {
                return "nonhispanic";
            } else if ( sr.equals( "UNKNOWN" ) | sr.equals( "* Ethnic Background *" )) {
                return "";
            }
        }
        return null;
    }

    public static String standardSex( Row row ) {
        String sex = "";
        String sr = row.getAs( "ARRESTEE SEX (10) eng" );
        if ( sr != null ) {
            if ( sr.equals( "FEMALE" ) ) {
                sex= "F";
            } else if ( sr.equals( "MALE" ) ) {
                sex= "M";
            }
        }
        return sex;
    }

    public static String standardAddress( Row row ) {
        String strnum = row.getAs( "Arrest Location St#" );
        String strname = row.getAs( "Arrest Location Whole Street Name" );
        String total = strnum + " " + strname;
        return total;
    }

}



