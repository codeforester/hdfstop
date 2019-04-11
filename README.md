# hdfstop

Parse HDFS logs and generate summaries.

# Contents

- hdfstop                   => main ruby script
- lib/hdfs_file_manager.rb  => class implementation for all log file reading / reporting logic
- lib/last_byte_position.rb => a small class for handling the management of last byte position read
- simulate_continous        => a Bash script that simulates the continuous mode
- hdfs-audit.log            => a sample HDFS log file
