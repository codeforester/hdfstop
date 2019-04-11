##
## This class manages the last byte position file
##
class LastBytePosition
    def initialize(file)
        if not File.exists?(file)
            raise
        end

        #
        # the last byte position inherits the same directory and name as the file being tracked.
        # in addition, we add the inode number and ".pos" extension
        #
        basename = File.basename(file)
        @position_file = "#{basename}-#{File.stat(file).ino}.pos"
        @position = 0
        @last_position_written = 0
    end

    #
    # read the last byte position file, store the position value in instance variable and return the same
    #
    def get
        begin
            pos_file = File.open(@position_file, "r")
            rescue
        end

        if not pos_file.nil?
            position = pos_file.gets.chomp
            pos_file.close
            @position = position.to_i
            return @position
        end

        return 0
    end

    #
    # return the byte position last returned by the get call
    #
    def get_last
        return @position
    end

    #
    # write byte position into file
    #
    def put(position)
        # don't update unless position has changed
        if position != @last_position_written
            pos_file = File.open(@position_file, "w")
            pos_file.write("#{position}\n")
            pos_file.close
            @last_position_written = position
        end
    end
end
