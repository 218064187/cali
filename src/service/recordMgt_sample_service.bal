import ballerina/grpc;
import ballerina/log;
import utils;

listener grpc:Listener ep = new (9090);

map<map<json>> RECORD_MAP = {};


service recordMgt on ep {

    resource function writeRecord(grpc:Caller caller, Record value) {
        string record_key = utils:get_key(value); //hash a record and return a key
        boolean exists = utils:has_key(utils:load_data(RECORD_MAP), record_key);
        string latest_version = "";

        if !exists {
            // Add a new record to the system
            latest_version = "1.0";
            value.key = record_key;
            RECORD_MAP[record_key][latest_version] = <@untainted> utils:to_json(value); //Module level variables are not allowed to contain tainted values unless it is annotated @tainted.
        }else{
            //if exists get a latest version to be used for response message
            latest_version = utils:get_latest_version(utils:load_data(RECORD_MAP), record_key).toString();
        }

        // create a response message
        Confirmation payload = {key: record_key,
                                'version: latest_version};

        // Send response to the caller.
        error? result = caller->send(payload);
        result = caller->complete();

        if result is error{
            log:printError("Error from Connector: " + result.reason() + " - "
                + <string>result.detail()["message"] + "\n");
        }

    }


    resource function updateRecord(grpc:Caller caller, Record value) {
        error? result = ();

        boolean has_given_a_version = value.'version != "";

        // check the Record that needs to be updated.
        if utils:has_key(utils:load_data(RECORD_MAP), value.key){
            if has_given_a_version{
                if !utils:has_version(utils:load_data(RECORD_MAP), value.key, value.'version){
                   // the key is correct but the version of a record is wrong
                    result = caller->sendError(grpc:NOT_FOUND, "Record cannot be found: version is unknown!");
                }

            }

            // Update the existing Record.
            Record rec = utils:record_parser(value);
            string record_key = utils:get_key(rec);
            string update_key = utils:get_parent_key(utils:load_data(RECORD_MAP), value.key);
            rec.key = record_key;
            //union type
            float|string new_version = utils:get_latest_version(utils:load_data(RECORD_MAP), update_key) + 0.1;
            new_version = new_version.toString().substring(0, 3);
            RECORD_MAP[update_key][new_version.toString()] = <@untainted> utils:to_json(rec);

            Confirmation payload = {key: record_key,
                                    'version: new_version.toString()};

            // Send response to the caller.
            result = caller->send(payload);
            result = caller->complete();

        }else{
            // Record not found error
            result = caller->sendError(grpc:NOT_FOUND, "Record does not exist!");
        }

        if result is error{
            log:printError("Error from Connector: " + result.reason() + " - "
                            + <string>result.detail()["message"] + "\n");
        }

    }


    resource function readRecord(grpc:Caller caller, ReadRequest value) {
        string record_key = value.key;
        string record_version = value.'version;
        error ? result = ();

        if  utils:has_key(utils:load_data(RECORD_MAP), value.key){
            Record payload = {};
            if (record_version == ""){ // the version is not given just get the latest Record
                payload = utils:get_latest_record(utils:load_data(RECORD_MAP), value.key);
            }else{
                // the version is given
                if  utils:has_version(utils:load_data(RECORD_MAP), record_key, record_version){
                    payload = utils:get_record_per_version(utils:load_data(RECORD_MAP), record_key, record_version);
                }else{
                //    send errors the version is unknown
                    result = caller->sendError(grpc:NOT_FOUND, "Record cannot be found: version is unknown!");
                }
            }


            // Send response to the caller.
            result = caller->send(payload);
            result = caller->complete();

        }else{
             // Send record not found error. the key is unknown
             string payload = "Record cannot be found.";
             result = caller->sendError(grpc:NOT_FOUND, payload);
        }

        if result is error{
            log:printError("Error from Connector: " + result.reason() + " - "
                            + <string>result.detail()["message"] + "\n");
        }

    }


    @grpc:ResourceConfig { streaming: true}
    resource function readRecordByCriterion(grpc:Caller caller, Criterion value) {
        Record[] records = utils:get_records_matching(utils:load_data(RECORD_MAP), value);
        grpc:Error? result = ();

        if (records.length() != 0){

            foreach Record rec in records{
                string rec_str = utils:record_str(utils:record_parser(rec));
                result = caller->send(rec_str);

                if(result is grpc:Error){
                    log:printError("Error from Connector: " + result.reason() + " - "
                                                    + <string>result.detail()["message"] + "\n");
                }else{
                    log:printInfo("Sent Record{key: "+ rec.key + "}");
                }
            }

            result = caller->complete();
            if (result is grpc:Error) {
                log:printError( "Error in sending completed notification to the caller",err=result);
            }

        }else{
            result = caller->sendError(grpc:INVALID_ARGUMENT, "No Record found matching provided criterion!");
            if (result is grpc:Error){
                log:printError( "Error in sending completed notification to the caller",err=result);
            }
        }

    }

}

public type Record record {|
    string date = "";
    Artist[] artists = [];
    string band = "";
    Song[] songs = [];
    string key = "";
    string 'version = "";
    
|};

public type Artist record {|
    string name = "";
    string member = "";
    
|};

public type Song record {|
    string title = "";
    string genre = "";
    string platform = "";
    
|};

public type ReadRequest record {|
    string key = "";
    string 'version = "";
    
|};

public type Criterion record {|
    string artist_name = "";
    string song_title = "";
    string band = "";
    string genre = "";
    
|};

public type Confirmation record {|
    string key = "";
    string 'version = "";
    
|};



const string ROOT_DESCRIPTOR = "0A0F7265636F72644D67742E70726F746F1207736572766963651A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F22AC010A065265636F726412120A046461746518012001280952046461746512290A076172746973747318022003280B320F2E736572766963652E41727469737452076172746973747312120A0462616E64180320012809520462616E6412230A05736F6E677318042003280B320D2E736572766963652E536F6E675205736F6E677312100A036B657918052001280952036B657912180A0776657273696F6E180620012809520776657273696F6E22340A0641727469737412120A046E616D6518012001280952046E616D6512160A066D656D62657218022001280952066D656D626572224E0A04536F6E6712140A057469746C6518012001280952057469746C6512140A0567656E7265180220012809520567656E7265121A0A08706C6174666F726D1803200128095208706C6174666F726D22390A0B526561645265717565737412100A036B657918012001280952036B657912180A0776657273696F6E180220012809520776657273696F6E22750A09437269746572696F6E121F0A0B6172746973745F6E616D65180120012809520A6172746973744E616D65121D0A0A736F6E675F7469746C651802200128095209736F6E675469746C6512120A0462616E64180320012809520462616E6412140A0567656E7265180420012809520567656E7265223A0A0C436F6E6669726D6174696F6E12100A036B657918012001280952036B657912180A0776657273696F6E180220012809520776657273696F6E32FC010A097265636F72644D677412350A0B77726974655265636F7264120F2E736572766963652E5265636F72641A152E736572766963652E436F6E6669726D6174696F6E12360A0C7570646174655265636F7264120F2E736572766963652E5265636F72641A152E736572766963652E436F6E6669726D6174696F6E12330A0A726561645265636F726412142E736572766963652E52656164526571756573741A0F2E736572766963652E5265636F7264124B0A15726561645265636F72644279437269746572696F6E12122E736572766963652E437269746572696F6E1A1C2E676F6F676C652E70726F746F6275662E537472696E6756616C75653001620670726F746F33";
function getDescriptorMap() returns map<string> {
    return {
        "recordMgt.proto":"0A0F7265636F72644D67742E70726F746F1207736572766963651A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F22AC010A065265636F726412120A046461746518012001280952046461746512290A076172746973747318022003280B320F2E736572766963652E41727469737452076172746973747312120A0462616E64180320012809520462616E6412230A05736F6E677318042003280B320D2E736572766963652E536F6E675205736F6E677312100A036B657918052001280952036B657912180A0776657273696F6E180620012809520776657273696F6E22340A0641727469737412120A046E616D6518012001280952046E616D6512160A066D656D62657218022001280952066D656D626572224E0A04536F6E6712140A057469746C6518012001280952057469746C6512140A0567656E7265180220012809520567656E7265121A0A08706C6174666F726D1803200128095208706C6174666F726D22390A0B526561645265717565737412100A036B657918012001280952036B657912180A0776657273696F6E180220012809520776657273696F6E22750A09437269746572696F6E121F0A0B6172746973745F6E616D65180120012809520A6172746973744E616D65121D0A0A736F6E675F7469746C651802200128095209736F6E675469746C6512120A0462616E64180320012809520462616E6412140A0567656E7265180420012809520567656E7265223A0A0C436F6E6669726D6174696F6E12100A036B657918012001280952036B657912180A0776657273696F6E180220012809520776657273696F6E32FC010A097265636F72644D677412350A0B77726974655265636F7264120F2E736572766963652E5265636F72641A152E736572766963652E436F6E6669726D6174696F6E12360A0C7570646174655265636F7264120F2E736572766963652E5265636F72641A152E736572766963652E436F6E6669726D6174696F6E12330A0A726561645265636F726412142E736572766963652E52656164526571756573741A0F2E736572766963652E5265636F7264124B0A15726561645265636F72644279437269746572696F6E12122E736572766963652E437269746572696F6E1A1C2E676F6F676C652E70726F746F6275662E537472696E6756616C75653001620670726F746F33",
        "google/protobuf/wrappers.proto":"0A1E676F6F676C652F70726F746F6275662F77726170706572732E70726F746F120F676F6F676C652E70726F746F62756622230A0B446F75626C6556616C756512140A0576616C7565180120012801520576616C756522220A0A466C6F617456616C756512140A0576616C7565180120012802520576616C756522220A0A496E74363456616C756512140A0576616C7565180120012803520576616C756522230A0B55496E74363456616C756512140A0576616C7565180120012804520576616C756522220A0A496E74333256616C756512140A0576616C7565180120012805520576616C756522230A0B55496E74333256616C756512140A0576616C756518012001280D520576616C756522210A09426F6F6C56616C756512140A0576616C7565180120012808520576616C756522230A0B537472696E6756616C756512140A0576616C7565180120012809520576616C756522220A0A427974657356616C756512140A0576616C756518012001280C520576616C756542570A13636F6D2E676F6F676C652E70726F746F627566420D577261707065727350726F746F50015A057479706573F80101A20203475042AA021E476F6F676C652E50726F746F6275662E57656C6C4B6E6F776E5479706573620670726F746F33"
        
    };
}

