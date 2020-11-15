import ballerina/grpc;
import ballerina/log;
import utils;

int total = 0;
public function main (string... args) {

    recordMgtBlockingClient blockingEp = new("http://localhost:9090");

    // Write a Record
    Record mumford_and_sons = {
        date: "22/10/2020",
        artists: [
            {
                name: "Winston Marshall",
                member: "yes"
            },
            {
                name: "Ben Lovett",
                member: "yes"
            },
            {
                name: "Baaba Maal",
                member: "no"
            }
        ],
        band: "Mumford & Sons",
        songs: [
            {
                title: "There will be time",
                genre: "folk rock",
                platform: "Deezer"
            }
        ]
    };

    // Create a new record
    write_record(blockingEp, mumford_and_sons);

    Record sam_martin = {
        date: "15/11/2020",
        artists: [
            {
                name: "Sam Martin",
                member: "yes"
            }
        ],
        band: "Sam Martin",
        songs: [
            {
                title: "Born With it",
                genre: "pop",
                platform: "Deezer"
            },
            {
                title: "The Storms",
                genre: "pop",
                platform: "Deezer"
            },
            {
                title: "It's Gonna Get Better",
                genre: "pop rock",
                platform: "Deezer"
            }
        ]
    };

     // Create a another record
    write_record(blockingEp, sam_martin);


    // Update a Record - providing a key only
    string mumford_and_sons_key = utils:get_key(mumford_and_sons); //get a key of a record to be updated
    Record mumford_and_sons_v2 = {
        date: "22/10/2020",
        artists: [
            {
                name: "Winston Marshall",
                member: "yes"
            },
            {
                name: "Ben Lovett",
                member: "yes"
            },
            {
                name: "Baaba Maal",
                member: "no"
            }
        ],
        band: "Mumford & Sons",
        songs: [
            {
                title: "There will be time",
                genre: "folk rock",
                platform: "Deezer"
            },
            {
                title: "Guiding Light",
                genre: "folk rock",
                platform: "Deezer"
            }
        ]
    };
    //add a new song (Guiding Light)
    mumford_and_sons_v2["key"] = mumford_and_sons_key;
    log:printInfo("::::::Update an Existing Record: (key only)");
    update_record(blockingEp, mumford_and_sons_v2);


    // Update a Record - providing a key and version
    string mumford_and_sons_v2_key = utils:get_key(mumford_and_sons_v2); //get a key of a record to be updated
    Record mumford_and_sons_v3 = {
        date: "22/10/2020",
        artists: [
            {
                name: "Winston Marshall",
                member: "yes"
            },
            {
                name: "Ben Lovett",
                member: "yes"
            },
            {
                name: "Ted Dwane",
                member: "yes"
            },
            {
                name: "Marcus Mumford",
                member: "yes"
            }
        ],
        band: "Mumford & Sons",
        songs: [
            {
                title: "There will be time",
                genre: "folk rock",
                platform: "Deezer"
            },
            {
                title: "Guiding Light",
                genre: "folk rock",
                platform: "Deezer"
            }
        ]
    };
    //replaced fake artists with real ones
    mumford_and_sons_v3["key"] = mumford_and_sons_v2_key;
    mumford_and_sons_v3["version"] = "1.1";
    log:printInfo("::::::Update an Existing Record: (key & version)");
    update_record(blockingEp, mumford_and_sons_v3);


    // Read a record providing a key only
    log:printInfo("::::::Read an Existing Record: (key only)");
    ReadRequest read_request_ms = {key: mumford_and_sons_key};
    read_record(blockingEp, read_request_ms);


    // Read a record providing a key and version
    log:printInfo("::::::Read an Existing Record: (key & version)");
    ReadRequest read_request_ms_v1 = {key: mumford_and_sons_key,
                                   'version: "1.0"};
    read_record(blockingEp, read_request_ms_v1);


     //Read a record providing criterion..
     Criterion criterion = {artist_name: "",
                            song_title: "The Storms",
                            band: "Mumford & Sons",
                            genre: ""};
     read_records_criterion(criterion);

}


 // Message listener for incoming messages
service recordMgtMessageListener = service {

    resource function onMessage(string rec) {
        //resource registered to receive server messages
        log:printInfo("Received: " + rec + "\n");
    }

    resource function onError(error err) {
        //resource registered to receive server error messages
        log:printError("Error from Connector: "+ err.reason() + " --  " +<string>err.detail()["message"]);
    }

    resource function onComplete() {
        //resource registered to receive server completed messages
        total = 1;
        log:printInfo("Server completes sending responses");
    }

 };

public function write_record(recordMgtBlockingClient blockingEp, Record rec){
    log:printInfo("::::::Write a new Record");
    //send a blocking writeRecord request to the caller
    var write_response =  blockingEp->writeRecord(rec);
    if (write_response is error){
        log:printError("Error from Connector: "+ write_response.reason() +
                       " - "+ <string>write_response.detail()["message"] + "\n");
    }else{
        Confirmation result;
        grpc:Headers res_headers;
        [result, res_headers] = write_response;
        string rec_key = result.key + ", ";
        string rec_version = result.'version+"}\n";
        log:printInfo("CREATED: {" + "Key: " + rec_key + "Version: " + rec_version);
    }
}

public function update_record(recordMgtBlockingClient blockingEp, Record rec){
    var update_response = blockingEp->updateRecord(rec);
    if (update_response is error){
        log:printError("Error from Connector: "+ update_response.reason() +
                       " - "+ <string>update_response.detail()["message"] + "\n");
    }else{
        Confirmation result;
        grpc:Headers res_headers;
        [result, res_headers] = update_response;
        string rec_key = result.key + ", ";
        string rec_version = result.'version+"}\n";
        log:printInfo("UPDATED: {" + "Key: " + rec_key + "Version: " + rec_version);

    }
}

public function read_record(recordMgtBlockingClient blockingEp, ReadRequest read_req){
    var read_response = blockingEp->readRecord(read_req);
    if (read_response is error){
        log:printError("Error from Connector: "+ read_response.reason() +
                       " - "+ <string>read_response.detail()["message"] + "\n");
    }else{
        Record result;
        grpc:Headers res_headers;
        [result, res_headers] = read_response;
        string rec_str = utils:record_str(result);
        log:printInfo("FOUND: " + rec_str + "\n");
    }
}

public function read_records_criterion(Criterion criterion){
    recordMgtClient nonBlockingEp = new("http://localhost:9090");
    log:printInfo("::::::Read an Existing Record(s): (criterion)");

    grpc:Error? readc_response = nonBlockingEp->readRecordByCriterion(criterion, recordMgtMessageListener);

    if (readc_response is grpc:Error){
        log:printError("Error from Connector: "+ readc_response.reason() +
                                    " - "+ <string>readc_response.detail()["message"] + "\n");
    }else{
        log:printInfo("Connected successfully");
    }

    while(total==0){}
    log:printInfo("Client got response successfully");
}