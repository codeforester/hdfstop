# hdfstop

Parse HDFS logs and generate summaries.

```
Usage: hdfstop [-h|--suppress_header] [-d|--debug] [-c|--continuous] [-t|--time_interval number] [-s|--sleep_interval number] -a|--audit_log file [-g|--group_by list] [-q|--query list]
    -a, --audit_log file             path to audit log file
    -g, --group_by list              list of fields for group by (default ugi,cmd)
    -l, --limit limit                output limit (default 10)
    -q, --query list                 query string in the form key1=value1,key2=value2...
    -c, --continuous                 continuously monitor the audit log
    -d, --debug                      run in debug mode
    -h, -—suppress_header             suppress header
    -t, --time_interval n            in continous mode, interval in seconds between two outputs (default 10)
    -s, --sleep_interval n           in continous mode, sleep time in seconds when EOF is reached (default 1)
```

# Contents

- hdfstop                   => main ruby script
- lib/hdfs_file_manager.rb  => class implementation for all log file reading / reporting logic
- lib/last_byte_position.rb => a small class for handling the management of last byte position read
- simulate_continous        => a Bash script that simulates the continuous mode
- hdfs-audit.log            => a sample HDFS log file
