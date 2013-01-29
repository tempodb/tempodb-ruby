module TempoDB
  class DataSet
    attr_accessor :series, :start, :stop, :data, :summary

    def initialize(series, start, stop, data=[], summary=nil)
      @series = series
      @start = start
      @stop = stop
      @data = data
      @summary = summary
    end

    def self.from_json(m)
      series = Series.from_json(m["series"])
      start = Time.parse(m["start"])
      stop = Time.parse(m["end"])
      data = m["data"].map {|dp| DataPoint.from_json(dp)}
      summary = Summary.from_json(m["summary"])
      new(series, start, stop, data, summary)
    end
  end
end

