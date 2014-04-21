module TempoDB
  # A search result from calling +find_data+. In addition to the
  # found DataPoint, contains the interval that the DataPoint was
  # found in.
  class DataPointFound
    attr_reader :interval, :predicate, :data, :tz

    def initialize(interval, predicate, data, tz)
      @interval = interval
      @predicate = predicate
      @data = data
      @tz = tz
    end

    def self.from_json(json)
      interval = json["interval"]
      predicate = json["predicate"]
      data = json["data"].map do |found|
        {
          "found" => DataPoint.from_json(found["found"]),
          "interval" => {
            "start" => Time.parse(found["interval"]["start"]),
            "end" => Time.parse(found["interval"]["end"])
          }
        }
      end
      tz = json["tz"]
      new(interval, predicate, data, tz)
    end
  end
end
