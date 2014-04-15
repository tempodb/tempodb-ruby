require 'spec_helper'

describe TempoDB::Client do
  it "encodes boolean values correctly" do
    stub_request(:get, "https://api.tempo-db.com/v1/series/?hello=true").
      to_return(:status => 200, :body => "{}", :headers => {})
    client = TempoDB::Client.new("key", "secret")
    client.list_series(:hello => true)
  end

  it "should not throw an exception when using SSL" do
    stub_request(:get, "https://api.tempo-db.com/v1/series/?key=my_key").
      to_return(:status => 200, :body => "{}", :headers => {})
    client = TempoDB::Client.new("key", "secret")
    client.list_series(:keys => "my_key")
  end

  describe "create_series" do
    it "creates a series by key name" do
      stub_request(:post, "https://api.tempo-db.com/v1/series/").
        to_return(:status => 200, :body => response_fixture('create_series.json'), :headers => {})
      keyname = "key2"
      client = TempoDB::Client.new("key", "secret")
      series = client.create_series(keyname)
      series.key.should == keyname
    end

    context "creating a series that already exists" do
      it "throws a TempoDBClientError exception" do
        stub_request(:post, "https://api.tempo-db.com/v1/series/").
          to_return(:status => 409, :body => response_fixture('create_series_non_unique.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        lambda { client.create_series("key2") }.should raise_error(TempoDB::TempoDBClientError)
      end
    end
  end

  describe "update_series" do
    it "adds the series name" do
      stub_request(:put, "https://api.tempo-db.com/v1/series/id/0e3178aea7964c4cb1a15db1e80e2a7f/").
        to_return(:status => 200, :body => response_fixture('update_series.json'), :headers => {})
      new_name = "my_series"
      series = TempoDB::Series.from_json(JSON.parse(response_fixture('create_series.json')))
      series.name.should == ""
      series.name = new_name
      client = TempoDB::Client.new("key", "secret")
      updated_series = client.update_series(series)
      updated_series.name.should == new_name
    end
  end

  describe "get_series" do
    it "fetches the series by key" do
      key = "012b7a1c5794423996e576792472b00b"
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/#{key}/").
        to_return(:status => 200, :body => response_fixture('get_series.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      series = client.get_series(key)
      series.key.should == key
    end
  end

  describe "find_data" do
    it "finds the closest datapoints" do
      key = "key1"
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/#{key}/find/?end=2012-01-02T00:00:00.000Z&predicate.function=first&predicate.period=1min&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => response_fixture('find_data.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      start = Time.utc(2012, 1, 1)
      stop = Time.utc(2012, 1, 2)
      found = client.find_data(key, start, stop, :predicate_function => "first", :predicate_period => "1min").to_a
      found[0]["found"].value.should == 1.23
      found[0]["interval"]["start"].should be_a(Time)
      found[0]["interval"]["end"].should be_a(Time)
    end
  end

  describe "aggregate_data" do
    it "aggregates across multiple series with rollups and interpolation" do
      stub_request(:get, "https://api.tempo-db.com/v1/segment/?aggregation.fold=max&end=2013-08-01T16:00:00.000-05:00&interpolation.function=linear&interpolation.period=1min&key=multi-key-4&rollup.fold=max&rollup.period=1hour&start=2013-08-01T04:00:00.000-05:00").
        to_return(:status => 200, :body => response_fixture('aggregate_data.json'), :headers => {})
      client = TempoDB::Client.new("client", "secret")
      start = Time.at(1375347600)
      stop = Time.at(1375390800)
      
      agg = client.aggregate_data("max", start, stop,
                                  :keys => ["multi-key-3", "multi-key-4"],
                                  :rollup_period => "1hour", :rollup_function => "max",
                                  :interpolation_period => "1min", :interpolation_function => "linear").to_a
      agg[0].value.should == 6.28
    end
  end

  describe "read_multi" do
    it "reads data from multiple series at once" do
       stub_request(:get, "https://api.tempo-db.com/v1/multi/?end=2013-08-01T16:00:00.000-05:00&interpolation.function=linear&interpolation.period=1min&key=multi-key-4&rollup.fold=max&rollup.period=1hour&start=2013-08-01T04:00:00.000-05:00").
        to_return(:status => 200, :body => response_fixture('read_multi.json'), :headers => {})

      client = TempoDB::Client.new("client", "secret")
      start = Time.at(1375347600)
      stop = Time.at(1375390800)
      
      multi = client.read_multi(start, stop,
                                :keys => ["multi-key-3", "multi-key-4"],
                                :rollup_period => "1hour", :rollup_function => "max",
                                :interpolation_period => "1min", :interpolation_function => "linear").to_a
      multi[0].value["multi-key-3"].should == 6.28
      multi[0].value["multi-key-4"].should == 2.34
    end
  end

  describe "get_summary" do
    it "retrieves the series summary for the designated key at the start and stop" do
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/multi-key-3/summary/?end=2013-08-01T16:00:00.000-05:00&start=2013-08-01T04:00:00.000-05:00&tz=CST").
        to_return(:status => 200, :body => response_fixture('get_summary.json'))
      client = TempoDB::Client.new("key", "secret")
      start = Time.at(1375347600)
      stop = Time.at(1375390800)

      summary = client.get_summary("multi-key-3", start, stop, :tz => "CST")
      summary.summary['count'].should == 2
      summary.summary['mean'].should == 4.71
      summary.summary['max'].should == 6.28
      summary.summary['stddev'].floor.should == 2
      summary.series['key'].should == 'multi-key-3'
    end
  end

  describe "read_multi_rollups" do
    it "calculates multiple rollups for a single series" do
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/multi-key-3/data/rollups/segment/?end=2013-08-01T16:00:00.000-05:00&interpolation.function=linear&interpolation.period=1min&limit=50&rollup.fold=avg&rollup.period=1hour&start=2013-08-01T04:00:00.000-05:00&tz=EST").
        to_return(:status => 200, :body => response_fixture('read_multi_rollups.json'))

      client = TempoDB::Client.new("key", "secret")
      start = Time.at(1375347600)
      stop = Time.at(1375390800)
      
      rollups = client.read_multi_rollups("multi-key-3", start, stop,
                                          :rollup_functions => ["first", "avg"], :rollup_period => "1hour",
                                          :interpolation_function => "linear", :interpolation_period => "1min",
                                          :tz => "EST",
                                          :limit => 50).to_a

      rollups[0].value["first"].should == 3.14
      rollups[0].value["mean"].should == 4.71
    end
  end

  describe "single_value" do
    it "returns one datapoint with the given search criteria" do
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/multi-key-3/single/?direction=nearest&ts=2013-08-01T04:00:00.000-05:00").
        to_return(:status => 200, :body => response_fixture('single_value.json'))
      client = TempoDB::Client.new("key", "secret")
      ts = Time.at(1375347600)
      value = client.single_value("multi-key-3", :direction => "nearest", :ts => ts)
      value.data.value.should == 3.14
    end
  end

  describe "multi_series_single_value" do
    it "returns a single value for multiple series given a certain search criteria" do
      stub_request(:get, "https://api.tempo-db.com/v1/single/?direction=nearest&key=multi-key-4&ts=2013-08-01T04:00:00.000-05:00").
        to_return(:status => 200, :body => response_fixture('multi_series_single_value.json'))
      client = TempoDB::Client.new("key", "secret")
      ts = Time.at(1375347600)
      values = client.multi_series_single_value(:keys => ["multi-key-3", "multi-key-4"],
                                                :direction => "nearest",
                                                :ts => ts).to_a
      values[0].series.key.should == "multi-key-3"
      values[0].data.value.should == 3.14
    end
  end

  describe "list_series" do
    context "with no options provided" do
      it "lists all series in the database" do
        stub_request(:get, "https://api.tempo-db.com/v1/series/?").
          to_return(:status => 200, :body => response_fixture('list_all_series.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        client.list_series.to_a.size.should == 7
      end
    end

    context "with filter options provided" do
      it "lists all series that meet the filtered criteria" do
        stub_request(:get, "https://api.tempo-db.com/v1/series/?key=key1&key=key2").
          to_return(:status => 200, :body => response_fixture('list_filtered_series.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        series = client.list_series(:keys => ["key1", "key2"]).to_a
        series.map(&:key).should == ["key1", "key2"]
      end
    end
  end

  describe "delete_series" do
    it "returns a delete summary with the deleted count" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/?key=key1&key=key2").
        to_return(:status => 200, :body => response_fixture('delete_series.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      summary = client.delete_series(:keys => ["key1", "key2"])
      summary.deleted.should == 2
    end
  end

  describe "read_data" do
    it "has an array of DataPoints" do
      start = Time.parse("2012-01-01 00:00 UTC")
      stop = Time.parse("2012-01-02 00:00 UTC")
      series_key = "key1"
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/#{series_key}/segment/?end=2012-01-02T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => response_fixture('read_id_and_key.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      set = client.read_data(series_key, start, stop).to_a
      set.size.should == 1440
    end

    it "handles special characters" do
      start = Time.parse("2012-01-01 00:00 UTC")
      stop = Time.parse("2012-01-02 00:00 UTC")
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/a%20b%5Ed&e%3Ff%2Fg/segment/?end=2012-01-02T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => response_fixture('read_id_and_key.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      set = client.read_data("a b^d&e?f/g", start, stop).to_a
      set.all? { |d| d.is_a?(TempoDB::DataPoint) }.should be_true
      set.size.should == 1440
    end

    context "with a series that does not exist" do
      it "throws a TempoDBClientError exception" do
        start = Time.parse("2012-01-01 00:00 UTC")
        stop = Time.parse("2012-01-02 00:00 UTC")
        stub_request(:get, "https://api.tempo-db.com/v1/series/key/non-existent/segment/?end=2012-01-02T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
          to_return(:status => 403, :body => "", :headers => {})
        client = TempoDB::Client.new("key", "secret")
        lambda { client.read_data("non-existent", start, stop).take(1) }.should raise_error(TempoDB::TempoDBClientError)
      end
    end
  end

  describe "write_key" do
    it "adds data points to the specific series key" do
      stub_request(:post, "https://api.tempo-db.com/v1/series/key/key3/data/").
        to_return(:status => 200, :body => "", :headers => {})
      points = [
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 0, 0), 12.34),
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 1, 0), 1.874),
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 2, 0), 21.52)
             ]
      client = TempoDB::Client.new("key", "secret")
      client.write_data("key3", points).should == {}
    end

    it "handles special characters" do
      stub_request(:post, "https://api.tempo-db.com/v1/series/key/a%20b%5Ed&e%3Ff/data/").
        to_return(:status => 200, :body => "", :headers => {})
      points = [
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 0, 0), 12.34),
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 1, 0), 1.874),
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 2, 0), 21.52)
             ]
      client = TempoDB::Client.new("key", "secret")
      client.write_data("a b^d&e?f", points).should == {}
    end
  end

  describe "write_multi" do
    it "writes multiple values to multiple series for different timestamps" do
      stub_request(:post, "https://api.tempo-db.com/v1/multi/").
        to_return(:status => 200, :body => "", :headers => {}).
        with { |request| request.body =~ /"t":"2013-09-12T01:00:00.000Z"/ }
      client = TempoDB::Client.new("key", "secret")

      client.write_multi do |multi|
        multi.add('0e3178aea7964c4cb1a15db1e80e2a7f', [TempoDB::DataPoint.new(Time.utc(2013, 9, 12, 1, 0), 4.164)])
        multi.add('key3', [TempoDB::DataPoint.new(Time.utc(2013, 9, 13, 1, 0), 324.991)])
      end
    end

    it "should return 207 on partial failure" do
      stub_request(:post, "https://api.tempo-db.com/v1/multi/").
        to_return(:status => 207, :body => response_fixture("multi_status_response.json"), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      lambda {
        client.write_multi do |multi|
          multi.add('0e3178aea7964c4cb1a15db1e80e2a7f', [TempoDB::DataPoint.new(Time.utc(2013, 9, 13, 1, 0), 4.164)])
          multi.add('', [])
        end
      }.should raise_error(TempoDB::TempoDBMultiStatusError)
    end

    it "accepts the write request as a write_multi argument" do
      stub_request(:post, "https://api.tempo-db.com/v1/multi/").
        to_return(:status => 200, :body => "", :headers => {}).
        with { |request| request.body =~ /"t":"2013-09-12T01:00:00.000Z"/ }
      client = TempoDB::Client.new("key", "secret")

      multi = TempoDB::MultiWrite.new
      multi.add('0e3178aea7964c4cb1a15db1e80e2a7f', [TempoDB::DataPoint.new(Time.utc(2013, 9, 12, 1, 0), 4.164)])
      multi.add('key3', [TempoDB::DataPoint.new(Time.utc(2013, 9, 13, 1, 0), 324.991)])
      client.write_multi(multi)
    end

    it "throws a TempoDBClientError if no block and no request is given" do
      client = TempoDB::Client.new("key", "secret")
      lambda { client.write_multi }.should raise_error(TempoDB::TempoDBClientError)
    end
  end

  describe "delete_data" do
    it "deletes a single point when the start and stop are the same time" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/key/key3/data/?end=2012-01-01T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      time = Time.utc(2012, 1, 1)
      client.delete_data("key3", time, time).should == {}
    end

    it "deletes a range of points between the start and stop" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/key/key3/data/?end=2012-01-02T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      start = Time.utc(2012, 1, 1)
      stop = Time.utc(2012, 1, 2)
      client.delete_data("key3", start, stop).should == {}
    end
  end
end
