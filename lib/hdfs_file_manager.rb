#
# This class manages the stats read from the HDFS log file
#
class HDFSLogFileManager
    def initialize(options, field_map, logger)
        @options    = options
        @field_map  = field_map
        @logger     = logger

        @last_print_time = Time.now
        @stats_hash = Hash.new                             # this is where we collect all stats
        @file_handle = File.open(options[:audit_log], "r")
        if @options.continuous
            #
            # In continuous mode, we seek to the last byte position read.
            # Also, we update the last byte position each time we print the stats in print_stats method.
            #
            @last_byte_pos = LastBytePosition.new(options.audit_log)
            @file_byte_position = @last_byte_pos.get

            @logger.debug "Seeking to last byte position #{@file_byte_position}"
            @file_handle.seek(@file_byte_position)
        end
    end

    #
    # read the log file, collect and print stats
    #
    def process_file
        while true
            @logger.debug "Starting read loop"
            while (line = @file_handle.gets) != nil
                #
                # Example line (tab separated):
                # 2016-05-06 18:03:22,983 INFO FSNamesystem.audit: allowed=true ugi=doe (auth:SIMPLE) ip=/10.211.55.1 cmd=contentSummary  src=/tmp/acme_company_datawarehouse/6f715539-9b50-438e-8b66-211ab54058ed  dst=null  perm=null proto=rpc
                #
                @logger.debug "line = #{line.chomp}"
                record_stats(line)
                if @options.continuous
                    @file_byte_position += line.length
                    print_stats
                end
            end # inner loop

            if @options.continuous
                @logger.debug "Sleeping for #{@options.sleep} seconds"
                sleep @options.sleep
                print_stats
            else
                print_stats
                break
            end
        end # outer loop
    end

    #
    # parse the input line passed, store it in a hash
    #
    def record_stats(line)
        @logger.debug "Doing record_stats"
        matched = true
        @options.query_array.each do |q|
            if not /#{q}/.match(line)
                matched=false
                break
            end
        end

        return if not matched;

        fields = line.chomp.split(/\t/)
        keys = []
        @options.group_by_array.each do |f|
            position, regex, replace_ex = @field_map[f]
            field = fields[position]
            field.gsub!(regex, replace_ex)
            keys << field
        end
        key = keys.join(',')
        if @stats_hash.has_key?(key)
            @stats_hash[key] += 1
        else
            @stats_hash[key] = 1
        end
    end

    #
    # sort the collected data and print it out in a pretty format
    #
    def print_stats
        now = Time.now
        return if @stats_hash.empty?
        return if @options.continuous and (now - @last_print_time < @options.interval)
        @last_print_time = now
        @logger.debug "Doing print_stats"

        #
        # print header and generate format array which would be looked up by index while printing
        #
        format_strings = []
        header = ""
        @options.group_by_array.each do |f|
            fs = @field_map[f][3]
            format_strings << fs
            header += sprintf "#{fs} ", f unless @options.suppress_header
        end

        unless @options.suppress_header
            header += sprintf " count\n"
            puts header
            header.size.times { print "-" }
            puts
        end

        #
        # sort the hash by value (count), descending
        #
        h = Hash[@stats_hash.sort_by {|k, v| -v}]

        #
        # traverse hash and print output
        #
        row_count = 0
        h.each_key do |k|
            index = 0
            k.split(/,/).each do |f|
                printf "#{format_strings[index]} ", f
                index += 1
            end
            printf "%6d\n", h[k]

            # output limit reached?
            row_count += 1
            break if row_count == @options.limit
        end

        if @options.continuous
            # now that we have printed the collected info, clear the stats for next round
            # and update last byte position
            reset_stats

            # update last byte position
            @logger.debug "Updating last byte position to #{@file_byte_position}"
            @last_byte_pos.put(@file_byte_position)
        end
    end

    def reset_stats
        @logger.debug "Doing reset_stats"
        @stats_hash.clear
    end

    def close
        @file_handle.close if not @file_handle.nil?
    end
end
