
Open Traffic Public Data Extract

Open Traffic Public Data Extracts are protocol buffer format. The format is described within a .proto file. This can be compiled into C++, Pythin, Java, Javascript to parse and access the protocol buffer files.

Tile Specification

Tiles are split up into three levels or hierarchies.  Hierarchy 0 contains segments pertaining to roads that are considered highway roads and are stored in 4 degree tiles.  Hierarchy 1 contains segments for roads that are at a arterial level and are saved in 1 degree tiles.  Finally, Hierarchy 2 contains segments that are considered at a local level.  These tiles are saved in .25 degree tiles.  For open traffic, we are only using levels 0 and 1.  

A tile id is determined by concatenating the tile index with the id of the tile.  For example, the tile id for the following speed tile `https://<Prefix URL>/1/037/740.spd.0.gz` is 37740.  The 1 in this example is the hierarchy level.  In this case, tile 740.spd.0.gz is located in level 1.  

Located within the speed tile bucket are speed and next segment tiles.  The first speed and next tile will have a 0 suffix, after that they will be numbered starting at 1. (e.g. tile.spd.0, tile.spd.1, tile.spd.2 or tile.nex.0, tile.nex.1, tile.nex.2)  These tiles are also stored in the following format: `year/week/level/tile index/tile id.[spd|nex].gz`.  So, again using the tile 740.spd.0.gz as an example, the URL is `https://<Prefix URL>/2017/01/1/037/740.spd.0.gz` for week 1 in 2017.  Note that the weeks begin at 1 and end at 52 or 53 as defined by [ISO_8601](https://en.wikipedia.org/wiki/ISO_8601).

Reference tiles are structured in the same manner as speed tiles; however, there is only one tile per tile id and they have no number in the suffix (e.g., `https://<Prefix URL>/1/037/740.ref.gz`) 

There are 3 separate tile sets within the Public Data Extract:
Historical Average Speeds
Intersection Delay and Queue
Reference Speeds
These all use the same .proto specification. Protocol buffers generally include "optional" message fields. One should always check for the presence of a particular message or data member prior to accessing. Different Public Data Extract tiles contain different data members.

Header and Summary Information
Each Public Data Extract tile contains header and summary information describing the contents of the file.

  //tile information, where this is
    optional uint32 level = 1;  //the tile level
    optional uint32 index = 2;  //the tile index

    //what segments will be found in this tile
    optional uint32 startSegmentIndex = 3; //the first segment index in the subtile
                                           //makes up the highest 21 bits of of the segment id
    optional uint32 totalSegments = 4;     //how many segments there are across all subtiles in this tile
                                           //these may be in this message or in another proto message
    optional uint32 subtileSegments = 5;   //the total number of segments in this subtile should be the same
                                           //for all subtiles except the last one might have less

    //time information, when this is
    //Note: that it represent both a single ordinal unit in time (if rangeEnd - rangeStart == unitSize)
    //but also be an average unit over a longer period of time (if rangeEnd - rangeStart > unitSize)
    optional uint32 rangeStart = 6; //epoch start time (inclusive) seconds
    optional uint32 rangeEnd = 7;   //epoch end time (exclusive) seconds
    optional uint32 unitSize = 8;   //target time range seconds, a week would be 604800
    optional uint32 entrySize = 9;  //target time range granularity seconds, an hour would be 3600
    optional string description = 10; //text describing the time period this covers

Average Speed Tiles

Average speed tiles contain average speeds along OSMLR segments for each hour of the wwek. There are also varianaces and prevalence (estimate of how prevalent the data is for this segment at each hour). Each of these mearures has 168 entries per segment.

    repeated uint32 speeds = 12 [packed=true];            //the average speed of each segment of each entry
    repeated uint32 speedVariances = 13 [packed=true];    //the variance between samples of each segment of each entry, also fixed precision
    repeated uint32 prevalences = 14 [packed=true];       //a rough indication of how many samples of ecah segment of each entry
    
A single array (keyword repeated) is used so that the data is compressed or packed within the protocol buffer. To further reduce file size, values are such that the all lie within a single byte. 

To index a particular hour within a segment the following equation is used to find the index within the array:
   int index = segment * 168 + hour

Intersection Delays and Queue Lengths

Reference Speed Tiles

message SpeedTile {
  
  //this allows us to chunk up large tiles into smaller pieces
  repeated SubTile subtiles = 1;
  message SubTile {
  
    optional uint32 deprecated=11;

    repeated uint32 speeds = 12 [packed=true];            //the average speed of each segment of each entry
    repeated uint32 speedVariances = 13 [packed=true];    //the variance between samples of each segment of each entry, also fixed precision
    repeated uint32 prevalences = 14 [packed=true];       //a rough indication of how many samples of ecah segment of each entry
    repeated uint32 nextSegmentIndices = 15 [packed=true];//an index into the next segment array of a given entry of a given segment
    repeated uint32 nextSegmentCounts = 16 [packed=true]; //total next segments for this segment of a given entry of a given segment

    repeated uint64 nextSegmentIds = 17 [packed=true];                  //full mask of tile and level and id from corresponding  entry
    repeated uint32 nextSegmentDelays = 18 [packed=true];               //delay in seconds from average speed from corresponding entry
    repeated uint32 nextSegmentDelayVariances = 19 [packed=true];       //variance of delay samples from corresponding entry
    repeated uint32 nextSegmentQueueLengths = 20 [packed=true];         //length of any queue on segment from corresponding entry
    repeated uint32 nextSegmentQueueLengthVariances = 21 [packed=true]; //variance of queue length samples from corresponding entry

    repeated uint32 referenceSpeeds20 = 22 [packed=true]; //20% are this speed or slower than this specific ref speed for each segment
    repeated uint32 referenceSpeeds40 = 23 [packed=true]; //40% are this speed or slower than this specific ref speed for each segment
    repeated uint32 referenceSpeeds60 = 24 [packed=true]; //60% are this speed or slower than this specific ref speed for each segment
    repeated uint32 referenceSpeeds80 = 25 [packed=true]; //80% are this speed or slower than this specific ref speed for each segment

}
