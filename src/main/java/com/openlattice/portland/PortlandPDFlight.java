package com.openlattice.portland;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class PortlandPDFlight {

    private static final Logger logger = LoggerFactory.getLogger( PortlandPDFlight.class );

    //which environment to send integration to
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    //Parse dates correctly, from input string columns. Offset from UTC.
    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_NewYork,
            "yyyy-MM-dd" );

    public static void main( String[] args ) throws InterruptedException {
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

                .addEntity( "PortlandPDArresteePerson" )  //variable name within flight. Doesn't have to match anything anywhere else
                    .to( "PortlandPDArresteePerson" )       //name of entity set belonging to
                    .addProperty( "nc.PersonGivenName", "Arrestee First Name" )           //arrest report number
                    .addProperty( "nc.PersonMiddleName", "Arrestee Middle Name" )
                    .addProperty( "nc.PersonSurName", "Arrestee Last Name" )
                    .addProperty( "nc.PersonRace" ).value( PortlandPDFlight::standardRace ).ok()
                    .addProperty( "nc.PersonEthnicity" ).value( PortlandPDFlight::standardEthnicity ).ok()
//                    .addProperty( "nc.SubjectIdentification" , "Arrest Charges Case #" )
                    .addProperty( "nc.PersonSex" ).value( PortlandPDFlight::standardSex ).ok()
                    .addProperty( "nc.PersonBirthDate" )
                    .value( row -> dtHelper.parse( row
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
//                    .addProperty( "nc.SubjectIdentification" , "Arresting Officer #" )
                .endEntity()

                .addEntity( "PortlandPDOfficerID" )
                    .to("PortlandPDOfficerID")
                    .addProperty( "publicsafety.officerid", "Arresting Officer #" )    //unique ID for address
                 .endEntity()
                .endEntities()
//
                .createAssociations()
                .addAssociation( "PortlandPDArrestedIn" )
                .to( "PortlandPDArrestedIn" )
                .fromEntity( "PortlandPDArresteePerson" )
                .toEntity( "PortlandPDIncident" )
                    .addProperty( "ol.arrestdatetime" )
                    .value( row -> dtHelper.parse( row
                            .getAs( "Arrest Date" ) ) )   //these rows are shorthand for a full function .value as below
                    .ok()
                    .addProperty( "location.address" ).value( PortlandPDFlight::standardAddress ).ok()
                    .addProperty( "ol.arrestcategory", "ARREST TYPE (186) eng" )
                    .addProperty( "criminaljustice.casenumber", "Arrest Charges Case #" )
                    .addProperty( "criminaljustice.nibrs", "UCR OFFN CODE (47) eng" )
                    .addProperty( "general.ChargeSequenceID", "Adult Arrest Charge Seq #" )
                    .addProperty( "ol.personsequencenumber", "Arrest Person Sequence Number" )
                    .addProperty( "ol.mapreference", "Map Ref# of Arst" )
                    .addProperty( "person.ageatevent", "Arrestee Age" )
                    .addProperty( "j.ArrestCharge", "ARREST STATE CHARGE (7) eng" )
                .endAssociation()

                .addAssociation( "PortlandPDArrestedBy" )
                .to("PortlandPDArrestedBy")
                .fromEntity("PortlandPDArresteePerson")
                .toEntity( "PortlandPDOfficerID" )
                .addProperty( "general.StringID", "Arrest Incident Number" )
                .endAssociation()

                .addAssociation( "PortlandPDPersonBecomesOfficial" )
                .to("PortlandPDPersonBecomesOfficial")
                .fromEntity( "PortlandPDOfficerPerson" )
                .toEntity( "PortlandPDOfficerID" )
                .addProperty( "nc.SubjectIdentification", "Arrest Incident Number" )
                .endAssociation()

                .endAssociations()
                .done();


        // add all the flights to flights
        flights.put( arFlight, arPayload );

        // Send your flight plan to Shuttle and complete integration
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launchPayloadFlight( flights );

        //return flights;
    }

    // Custom Functions for Parsing CAD number from casenum - modified from functions for splitting first and last names

    public static String standardRace( Row row ) {
        String sr = row.getAs( "ARRESTEE RACE (11) eng" );
        if ( sr != null ) {
            if ( sr.equals( "ASIAN OR PACIFIC ISLANDER" ) ) {
                return "asian/pacisland";
            } else if ( sr.equals( "BLACK" ) ) {
                return "black";
            } else if ( sr.equals( "UNKNOWN" ) | sr.equals( "HISPANIC" ) | sr.equals( "AMERICAN INDIAN / ALASKAN NATIVE" ) | sr.equals( "* Race *" )) {
                return "";
            } else if ( sr.equals( "WHITE" ) ) {
                return "white";
            }
        }
        return null;
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
        String sr = row.getAs( "ARRESTEE SEX (10) eng" );
        if ( sr != null ) {
            if ( sr.equals( "FEMALE" ) ) {
                return "F";
            } else if ( sr.equals( "MALE" ) ) {
                return "M";
            } else if ( sr.equals( "UNKNOWN" ) | sr.equals( "* Sex *" )) {
                return "";
            }
        }
        return null;
    }

    public static String standardAddress( Row row ) {
        String strnum = row.getAs( "Arrest Location St#" );
        String strname = row.getAs( "Arrest Location Whole Street Name" );
        return strnum + strname;
    }

}



