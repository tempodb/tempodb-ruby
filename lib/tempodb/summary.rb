module TempoDB
  class Summary
    def self.from_json(m)
      summary = Summary.new
      m.each do |k, v|
        summary.instance_variable_set("@#{k}", v)  ## create and initialize an instance variable for this key/value pair
        summary.class.send(:define_method, k, proc{self.instance_variable_get("@#{k}")})  ## create the getter that returns the instance variable
        summary.class.send(:define_method, "#{k}=", proc{|v| self.instance_variable_set("@#{k}", v)})  ## create the setter that sets the instance variable
      end
      summary
    end
  end
end

