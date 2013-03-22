ss_get_mysql_stats.py
================================

*Purpose: Retrieve various parameters from a running MySQL instance*

Install Requirements
-------------------------------
`pip install -r requirements.txt`

###Usage:
ss_get_mysql_stats.py \[-h\] --host HOST \[--port PORT\] \[--user USER\] \[--password PASSWORD\] \[--heartbeat HEARTBEAT\] \[--no-replication-client\] \[--no-super\] \[--nocache\] \[--graphite-host GRAPHITE_HOST\] \[--graphite-port GRAPHITE_PORT\] \[--use-graphite\]

###Sample Usage:  
`ss_get_mysql_stats.py --host localhost --user root `

###Allowed arguments:  
    -h, --help            show this help message and exit  
    --host HOST           Hostname to connect to (default: None)  
    --port PORT           Database Port (default: 3306)  
    --user USER           MySQL username (default: cactiuser)  
    --password PASSWORD   MySQL password (default: cactiuser)  
    --heartbeat HEARTBEAT MySQL heartbeat table (see pt-heartbeat) (default: )  
    --no-replication-client Prevent the queries needing "REPLICATION CLIENT" from running (default: False)  
    --no-super            Prevent the queries needing "SUPER" from running (default: False)  
    --nocache             Do not cache results in a file (default: False)  
    --graphite-host GRAPHITE_HOST Graphite host (default: 127.0.0.1)  
    --graphite-port GRAPHITE_PORT Graphite port (default: 8080)  
    --use-graphite        Send stats to Graphite (default: False)  

*Note: RDS users will probably need to use both the --no-replication-client and --no-super flags to prevent errors.*
