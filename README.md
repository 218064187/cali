
**cali**, a __storage system__ for songs released by an artist or a group of artists. Each record stored in the system will be saved in a JSON file. A record can be represented as follows:
```json
{
	date: "22/10/2020",
	artists: [
		{
			name: "Winston Marshall",
			member: yes
		},
		{
			name: "Ben Lovett",
			member: yes
		},
		{
			name: "Baaba Maal",
			member: no
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
}
```

The architecture of the system comprises a *client* and a *server* that communicate following a remote invocation paradigm. The following operations should be allowed in the system:

1. write a new record
2. update a record
3. read a record

To write a record, the client invokes a remote operation on the server and passes the record as an argument. Once the request is received, the server **hashes** the record using a __hash function__. The resulting hash code represents the key that will be attached to the record. To avoid duplication, the server checks whether such a record exists in the system. If not, it saves the record in the file and returns the key and a version number to the client. If on the contrary, the record already exists in the system, the server returns the key and the latest version attached to the key. A version is an additional identifier for a record. Its role is to support updates of records.

To update a record, the client sends the key attached to the record, a version and the modified copy of the record. When the version or the key passed by the client is unknown to the server, it returns an error with the message "Record does not exist!". When both the key and the version are known to the server, it saves the modified copy in the file with a new version attached to it. The newly created version becomes a node successor to the version passed by the client. Simply put, versioning should be handled like a **direct acyclic graph**. When the update operation is successful, the server returns the key and the newly generated version.

Finally, several options are considered for the client to read a record:
* If the client passes a key, it receives the record corresponding to the latest version depending on that key. When there is no such record, an error message "Record does not exist" is returned.
* If the client passes a key and a version, the record corresponding the combination of key and version is returned, if it exists. If not an error message is returned.
* If the client passes a criterion or a combination thereof, the server **streams** back all records satisfying the criteria. A criterion could be the name of an artist, the name of a band and the title of a song. When several criteria are combined, a disjunction of the constituting parts should be assumed. For example, if the client sends a criterion that includes the title of a song or the name of an artist, all records that contain either the name of the artist or the title of the song should be streamed back to the client in response. To improve the search time of a record, particularly when using criteria, you might consider implementing a *secondary index* that helps you locate records faster following the criteria.

Your task is to implement **cali** using gRPC as the remote invocation tool in the Ballerina programming language. More specifically, you will:
1. define the interface of the remote operations using __Protocol Buffer__;
2. generate the stubs on both the client and server;
3. implement both the client and the server.

Note that you can use the __crypto__ module to implement your hash function.
