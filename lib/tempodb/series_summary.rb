module TempoDB
  # Used to return common aggregate metadata for datapoint reads
  class SeriesSummary
    attr_accessor :summary, :series, :start, :stop, :tz

    def initialize(summary, series, start, stop, tz)
      @summary = summary
      @series = series
      @start = start
      @stop = stop
      @tz = tz
    end

    def self.from_json(json)
      new(json["summary"], json["series"], Time.parse(json["start"]), Time.parse(json["end"]), json["tz"])
    end
  end
end

