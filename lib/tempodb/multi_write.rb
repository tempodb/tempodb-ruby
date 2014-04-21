module TempoDB
  # Used to send collections of datapoints to multiple Series
  class MultiWrite
    attr_reader :series

    def initialize
      @series = []
    end

    # Add a an array of DataPoint to the specific +series_key+
    #
    # * +series_key+ [String] - The key to write to
    # * +data+ [Array] - An array of DataPoint
    #
    def add(series_key, data)
      data.each do |series|
        @series << { "key" => series_key }.merge(series.to_json)
      end
    end
  end
end
