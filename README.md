Done:
1.Design the input, output, architecture for the sink
https://wiki.cask.co/display/CE/Multiple+File+Set+Sink

2.Support Avro Output Format

3.Customized FileBachSink

4.Customized RecordWriter that can write to one file

5.Input csv:
```
id,first_name,last_name,sex,address,salary
201EMPLPIM,Sean,Froula,M,"2105 8th St, Uhome, WY",7000
202EMPLPIM,Madeline,Heine,F,"22 Rochester St, Uhome, WY",5000
202EMPLPIM,Margaret,Morehead,F,"100 Commerce Cr, Springfield, IL",6000
203EMPLPIM,Jennifer,Costello,F,"21 Walker Rd, Uhome, WY",8000
```

6.Parse JSON Input into designed class, following is the fileset properties:
```

{
  "name": "Multiple File Set Sink",
  "outputFileSets": [
    {
      "compressionCodec": "Snappy",
      "datasetName": "fileset0",
      "datasetTargetPaths": "",
      "expression": "(id.startsWith(\"201\") || id.startsWith(\"202\") || id.startsWith(\"203\") )&& salary >= 5000 && salary <= 7000",
      "filesetProperties": "",
      "schema": [
          {
            "name": "id",
            "type": ["string","null"]
          },
          {
            "name": "first name",
            "type": ["string","null"]
          },
          {
            "name": "last name",
            "type": ["string","null"]
          },
          {
            "name": "sex",
            "type": ["string","null"]
          },
          {
            "name": "address",
            "type": ["string","null"]
          },
          {
            "name": "salary",
            "type": ["string","null"]
          }
        ]
    },
    {
      "compressionCodec": "Snappy",
      "datasetName": "fileset1",
      "datasetTargetPaths": "",
      "expression": " id.startsWith(\"202\") && salary >= 5000 && salary <= 7000",
      "filesetProperties": "",
      "schema":[
          {
            "name": "id",
            "type": ["string","null"]
          },
          {
            "name": "first name",
            "type": ["string","null"]
          },
          {
            "name": "salary",
            "type": ["string","null"]
          }
        ]
    },
    {
      "compressionCodec": "Snappy",
      "datasetName": "fileset2",
      "datasetTargetPaths": "",
      "expression": "(id.startsWith(\"202\") || id.startsWith(\"203\")) && salary >= 5000 && salary < 9000",
      "filesetProperties": "",
      "schema": [
          {
            "name": "id",
            "type": ["string","null"]
          },
          {
            "name": "first name",
            "type": ["string","null"]
          },
          {
            "name": "sex",
            "type": ["string","null"]
          },
          {
            "name": "address",
            "type": ["string","null"]
          },
          {
            "name": "salary",
            "type": ["string","null"]
          }
        ]
    }
  ],
  "type": "batchsink"
}
```
7.extract out file-path and fields, and parse other file properties

To-do:

8.Using test to debug the sink.
References:
```
https://github.com/hydrator/kafka-plugins/blob/develop/src/test/java/co/cask/hydrator/PipelineTest.java#L114

https://github.com/caskdata/hydrator-plugins/blob/develop/core-plugins/src/test/java/co/cask/hydrator/plugin/batch/ETLTPFSTestRun.java
```

9.Customized RecordWriter that can switch writeout stream base on  path/dataset name and write to them
```
https://github.com/hydrator/multiple-fileset-sink/blob/master/src/main/java/co/cask/hydrator/plugin/MultipleSnapshotFilesetSinkOutputFormat.java
```

