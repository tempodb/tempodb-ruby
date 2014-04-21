module TempoDB
  # A composite type that represents multiple datapoints at a single timestamp
  class MultiPoint
    attr_accessor :ts, :value

    def initialize(ts, value)
      @ts = ts
      @value = value
    end

    def self.from_json(m)
      ts = Time.parse(m["t"])
      value = m["v"]
      new(ts, value)
    end

    def [](key)
      value[key]
    end
  end
end
