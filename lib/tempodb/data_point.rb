module TempoDB
  # The fundamental unit of TempoDB. A compound type that holds both:
  #
  # * +ts+ [Time] - A timestamp
  # * +value+ [Integer/Float] - A numeric value
  #
  class DataPoint
    attr_reader :ts, :value

    def initialize(ts, value)
      @ts = ts
      @value = value
    end

    def to_json(*a)
      {"t" => ts.iso8601(3), "v" => value}
    end

    def self.from_json(m)
      new(Time.parse(m["t"]), m["v"])
    end
  end
end

