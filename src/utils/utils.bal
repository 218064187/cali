import ballerina/crypto;
import ballerina/lang.'float as floats;


// HELPER FUNCTIONS

public function get_key(Record data) returns string{
    // hash a record using ballerina/crypto:hashSha256 hash function
    // returns a result of a hash-function as a string
    string rec_data = to_string(record_parser(data)); //converts a Record to string
    byte[] record_hash = crypto:hashSha256(rec_data.toBytes());
    string record_key = <string> record_hash.toBase16();
    return record_key;
}

public function get_parent_key(map<map<Record>> record_map, string record_key) returns string {
    // gets the parent key or the original key of the first ever created record before updates
    string parent_key = "";
    if record_map.hasKey(record_key){
        parent_key = record_key;
    }else{
        foreach var [p_key, records] in record_map.entries(){
            foreach var rec in records{
                if rec.key == record_key{
                    parent_key = p_key;
                }
            }

        }
    }
    return parent_key;
}

public function get_latest_record(map<map<Record>> record_map, string record_key) returns Record{
    // return latest(newly update) Record if the key is available
    Record data = {};

    if has_key(record_map, record_key){
        string parent_key = get_parent_key(record_map, record_key);
        string latest_version = get_latest_version(record_map, record_key).toString();
        Record? rec = record_map[parent_key][latest_version];
        if rec is Record{
            data = rec;
        }
    }
    return data;

}

public function get_record_per_version(map<map<Record>> record_map, string record_key, string record_version) returns Record{
    // returns a Record matching the version given
    Record data = {};
    if has_version(record_map, record_key, record_version){
        string parent_key = get_parent_key(record_map, record_key);
        Record? rec = record_map[parent_key][record_version];
        if rec is Record{
            return rec;
        }

    }
    return data;
}

public function get_records(map<map<Record>> record_map) returns map<Record>{
    // returns latest records only
    map<Record> records = {};
    foreach var recs in record_map{
        int len = recs.length();
        string record_key = recs.keys()[len-1];
        Record? rec = recs[record_key];
        if rec is Record{
            records[record_key] = rec;
        }

    }
    return records;
}

public function get_records_matching(map<map<Record>> record_map, Criterion criterion)returns Record[]{
    // returns a list of Records matching specified properties
    map<Record> records = {};
    foreach var rec in get_records(record_map){
        if rec.band.toLowerAscii() == criterion.band.toLowerAscii(){
            records[rec.key] = rec;
        }
        // checks an artist that has a name matching
        foreach var artist in rec.artists{
            if artist.name.toLowerAscii() == criterion.artist_name.toLowerAscii(){
                records[rec.key] = rec;
            }
        }

        // checks a song that has title matching
        foreach var song in rec.songs{
            if (song.title.toLowerAscii() == criterion.song_title.toLowerAscii() ||
                song.genre.toLowerAscii() == criterion.genre.toLowerAscii()){
                records[rec.key] = rec;
            }
        }
    }
    return to_list(records);
}

public function get_latest_version(map<map<Record>> record_map, string record_key) returns float{
    // gets a latest  version number of a record
    string latest_version = "";

    // scenario were no updates were made yet
    if has_key(record_map, record_key){
        int len = 0;
        string parent_key = get_parent_key(record_map, record_key);
        map<Record>? recs = record_map[parent_key];
        if recs is map<Record>{
            len = recs.length();
            latest_version = recs.keys()[len-1];
        }
    }
    return version_tofloat(latest_version);
}


// CONVERSION

public function version_tofloat(string record_version) returns float{
    // Converts a record version to a float
    return <float> floats:fromString(record_version);
}

public function to_string(Record data) returns string{
    // converts json to string
    return to_json(data).toString();
}

public function to_json(Record data) returns json{
    // converts a Record<record> obj to Record<json> obj
    json rec_json = {};
    var values = typedesc<json>.constructFrom(data);
    if values is json{
        rec_json = values;
    }
    return rec_json;
}

public function from_json(json data) returns Record{
    // converts Record<json> obj to Record<record> obj
    Record rec = {};
    var values = typedesc<Record>.constructFrom(data);
    if values is Record{
        rec = values;
    }
    return rec;
}

public function to_list(map<Record> record_map) returns Record[]{
    // returns a list of records
    Record[] records = [];

    foreach var rec in record_map{
        records.push(rec);
    }
    return records;
}



// CHECKS

public function has_key(map<map<Record>> record_map, string record_key) returns boolean{
    // checks if a given key is available

    if record_map.hasKey(record_key){
        return true;
    }else{
        // checks if it available in other versions
        foreach var records in record_map{
            foreach var rec in records{
                if rec.key == record_key{
                    return true;
                }
            }
        }
    }
    return false;
}

public function has_version(map<map<Record>> record_map, string record_key, string record_version) returns boolean{
    // checks if a record has a version given
    if has_key(record_map, record_key){
        string parent_key = get_parent_key(record_map, record_key);
        map<Record>? records = record_map[parent_key];
        if records is map<Record>{
            if records.hasKey(record_version){
                return true;
            }
        }
    }
    return false;
}

//Others

public function record_parser(Record rec) returns Record{
    // returns a new record without key & version number attached to it
    return {
        date: rec.date,
        artists: rec.artists,
        band: rec.band,
        songs: rec.songs
    };
}

public function load_data(map<map<json>> record_map) returns map<map<Record>>{
    // loads data from map<map<json>> to map<map<Record>>
    map<map<Record>> records = {};
    foreach var [rec_key, recs] in record_map.entries(){
        foreach var [rec_version, rec] in recs.entries(){
            records[rec_key][rec_version] = from_json(rec);
        }
    }
    return records;

}

public function record_str(Record data) returns string{
    // returns a nicely string representation of a Record without a key and version number
    Record rec = record_parser(data);
    string songs = ", songs:[";
    string artists = ", artists:[";
    foreach Song song in rec.songs{
        string song_str = "Song{title:" + song.title + ", genre:" + song.genre + ", platform:"+song.platform + "}, ";
        songs = songs + song_str;
    }
    songs = songs + "]";

    foreach Artist artist in rec.artists{
        string artist_str = "Artist{name:" + artist.name + ", member:" + artist.member + "}, ";
        artists = artists + artist_str;
    }
    artists = artists + "]";
    return "Record{date:" + rec.date + artists+ ", band:"+ rec.band + songs;
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


