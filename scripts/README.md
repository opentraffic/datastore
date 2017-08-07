# Open Traffic - create_data_extract.py

This loads data from 2 sources, osmlr & flatbuffer files from the datastore output AWS s3 bucket.  They loaded to read in the segment ids, speeds & next segment id info that
will then generate the segment speed json files to be consumed by the UI.

Level 0 & 1 are only used as input right now.  

File structure: 2017/<week ie. 0>/<level ie. 0>/<tileid ie. 002>/<tile.json ie. 415.json>...

#### Abbreviated Key Parameters (showing verbose name)

Key parameters now have abbreviated names.  See below...

{
    “rt” | "range_type": "ordinal",
    “ut”| "unit_type": “hour",
    “y”| “year”: 2017,
    “wt”| “week”: 0,
    “ue”| "unit_entries": 168,
    “d”| "description": "168 ordinal hours of week 0 of year 2017",
    “segs”| "segments": {
   	  "123456789": {
        “rsp” | “reference_speed”:37,
        "ets” | “entries”:[{
            "e” | "entry": 0,
       		"id” |  "id": "123456789",
       		“sp” | "speed": 25,
            "spv” | “speed_variance”: 1.0,
     		“p” | "prevalence": 7,
     		“nsegs” |  "next_segments": {
     		  "987654321": {
                “p” |  “prevalence”:3,
     			“id” | "id": "987654321",
                “dv” | “delay_variance”: 10,
                “qv” | “queue_variance”: 1.0,
              	“ql” | “queue_length": 50
              },
     		  "192837465": {
                “p” |  “prevalence”:3,
     		    “id” |  "id": "192837465",
                “dv” | “delay_variance”: 10,
                “qv” | “queue_variance”: 1.0,
                “ql” | “queue_length": 50
     		  }
     		}
   		  },
   		    null,
   		  {
   		    “e” | "entry": 2,
   			"id” |  "id": "123456789",
            “sp” | "speed": 25,
   			“spv” | "speed_variance": 1.0,
   			“p” | "prevalence": 6,
   			“nsegs” | "next_segments": {
   			  "987654321": {
   			      “p” |  “prevalence”:3,
   				  “id” | "id": "987654321",
                  “dv” | “delay_variance”: 20,
                  “qv” | “queue_variance”: 1.0,
                  “ql” | “queue_length": 10
   			  },
   			  "192837465": {
   				  “p” |  “prevalence”:3,
   				  “id” | "id": "192837465",
                  “dv” | “delay_variance”: 12,
                  “qv” | “queue_variance”: 2.0,
                  “ql” | “queue_length": 15
   			  }
   			}
   		  },
          null,
        ]}
      }
    }
}

