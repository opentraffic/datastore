# Open Traffic Public Data Extract

Open Traffic Public Data Extracts are protocol buffer format. The format is described within a .proto file. This can be compiled into C++, Python, Java, Javascript to parse and access the protocol buffer files.

### Tile Specification

Public Data Extract data is divided into geographic regions or tiles. The tiles are also broken up by the road hierarchy level. The tiling and road hierarchy system is described [here](https://github.com/valhalla/valhalla-docs/blob/master/tiles.md).

A tile id is determined by concatenating the tile index directory with the id of the tile.  For example, the tile id for the following speed tile `https://<Prefix URL>/1/037/740.spd.0.gz` is 37740.  The 1 in this example is the hierarchy level.  In this case, tile 740.spd.0.gz is located in level 1.  

Located within the speed tile bucket are speed and next segment tiles.  The first speed and next tile will have a 0 suffix, after that they will be numbered starting at 1. (e.g. tile.spd.0, tile.spd.1, tile.spd.2 or tile.nex.0, tile.nex.1, tile.nex.2)  These tiles are also stored in the following format: `year/week/level/tile index/tile id.[spd|nex].gz`.  So, again using the tile 740.spd.0.gz as an example, the URL is `https://<Prefix URL>/2017/01/1/037/740.spd.0.gz` for week 1 in 2017.  Note that the weeks begin at 1 and end at 52 or 53 as defined by [ISO_8601](https://en.wikipedia.org/wiki/ISO_8601).

Reference tiles are structured in the same manner as speed tiles; however, there is only one tile per tile id and they have no number in the suffix (e.g., `https://<Prefix URL>/1/037/740.ref.gz`)

There are 3 separate tile sets within the Public Data Extract:
* Historical Average Speeds (.spd tiles)
* Intersection Delay and Queue Lengths (.nex tiles)
* Reference Speeds (.ref tiles)

These all use the same .proto specification. Protocol buffers generally include "optional" message fields. One should always check for the presence of a particular message or data member prior to accessing. Different Public Data Extract tiles contain different data members.

### Header and Summary Information
Each Public Data Extract tile contains header and summary information describing the contents of the file. This includes tile information, a description of the traffic segments found within this protocol buffer file (for multi-part protocol buffer representations), and a description of the time period supported within this file. The individual entries include:

| Summary message | Description |
| :--------- | :----------- |
| `level` | The tile hierarchy level. |
| `index` | The tile index within the hierarchy level. |
| `startSegmentIndex` | The first segment index in the subtile. This index makes up the highest 21 bits of of the segment Id. |
| `totalSegments` | How many segments there are across all subtiles in this tile. These may be in this message or in another protocol buffer message. |
| `subtileSegments` | The total number of segments in this subtile. This should be the same for all subtiles except the last one might have less. |
| `rangeStart` | Epoch start time (inclusive) seconds. |
| `rangeEnd` | Epoch end time (exclusive) seconds. |
| `unitSize` | Target time range in seconds. For example, one week would be 604800. |
| `entrySize` | Target time range granularity in seconds. For example, one hour would be 3600. |
| `description` | Text describing the time period this covers. |

### Subtiles

Subtiles are created so that we can break up the large tile into smaller tiles, keeping the tiles in the single-digit megabyte range.  This will help a lot once all segments have full data.
The subtiles are broken up into a maximum of 10,000 segments.  All of the subtiles will contain 10,000 segments except for the final one, which will be the difference of (10,000 - `totalSegments`).
The total of all the `subtileSegments` will equal the `totalSegments`.


### Historical Average Speed Tiles

Average speed tiles contain average speeds along OSMLR segments for each hour of the week. There are also variances and prevalence (estimate of how prevalent the data is for this segment at each hour). Each of these measures has 168 entries per segment.

| Summary message | Description |
| :--------- | :----------- |
| `speeds` | The average speed of each segment of each entry (time period). A value of 0 indicates there were not enough samples for an entry to compute an average speed. |
| `speedVariances` | The variance between samples of each segment of each entry. This field is fixed precision. (TBD - describe precision!). |
| `prevalences` | A rough indication of how many samples exist for each segment, for each entry. This is a value from 1 to 10, where 1 indicates few samples, and 10 indicates many samples. This value is purposely rough, to help preserve privacy. |
| `nextSegmentIndices` | An index into the next segment array of a given entry of a given segment.|
| `nextSegmentCounts` | The total next segments for this segment of a given entry of a given segment.|

A single array (keyword repeated) is used so that the data is compressed or packed within the protocol buffer. To further reduce file size, values are such that the all lie within a single byte.

To index a particular hour within a segment the following equation is used to find the index within the array:
* int index = segment index * 168 + hour

### Intersection Delays and Queue Lengths

The Next Segment Tiles contain the intersection delays and queue lengths along the OSMLR next segments for each hour of the week.  At an intersection, one or multiple next segments can be tied/paired to a segment. The segments' `nextSegmentCounts` will tell you the total number of next segments that should be associated to it.  The segments' `nextSegmentIndices` will tell you the index into the next segment array so that you can get the corresponding delay, queue length, etc.    

| Summary message | Description |
| :--------- | :----------- |
| `nextSegmentIds` | This is a list of all next segment Ids. This is a full mask of tile, level, and Id from corresponding entry. |
| `nextSegmentDelays` | Average delay in seconds when transitioning from one segment onto the next segment. |
| `nextSegmentDelayVariances` | Variance of delay samples from corresponding entry. |
| `nextSegmentQueueLengths` | Length of any queue on the segment when transitioning to the next segment. |
| `nextSegmentQueueLengthVariances` | Variance of queue length samples from corresponding entry. |

### Reference Speed Tiles

The reference speed tiles provide average speeds across time periods (generally hours) for which average speed data exists (includes all speed tiles across all time periods). It also includes "reference speeds" which provide a rough approximation of the distribution of average speeds across all hours for which average speed data exists.

| Summary message | Description |
| :--------- | :----------- |
| `speeds` | The average speed of each segment of each entry (time period). A value of 0 indicates there were no average speeds for this segment across all time periods. |
| `referenceSpeeds20` | 20% of average speeds across all time periods are slower than or equal to this specific reference speed. This is repeated across each segment. |
| `referenceSpeeds40` | 40% of average speeds across all time periods are slower than or equal to this specific reference speed. This is repeated across each segment. |
| `referenceSpeeds60` | 60% of average speeds across all time periods are slower than or equal to this specific reference speed. This is repeated across each segment. |
| `referenceSpeeds80` | 80% of average speeds across all time periods are slower than or equal to this specific reference speed. This is repeated across each segment. |
