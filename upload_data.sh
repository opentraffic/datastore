!/bin/bash

curl -vs --data-urlencode 'json={"segments": [{"segment_id"ment_id": 356789,"start_time": 98765,"end_time": 98777,"length":555},{"segment_id": 345780,"start_time": 98767,"end_time": 98779,"length":678},{"segment_id": 345795,"prev_segment_id": 656784,"start_time": 98725,"end_time": 98778,"length":479}, {"segment_id": 545678,"prev_segment_id":556789,"start_time": 98735,"end_time": 98747,"length":1234}],"provider":123456,"mode": "auto"}' -G http://localhost:8003/store
