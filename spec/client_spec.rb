require 'spec_helper'

describe TempoDB::Client do
  it "should not throw an exception when using SSL" do
    stub_request(:get, "https://api.tempo-db.com/v1/series/?key=my_key").
      to_return(:status => 200, :body => "{}", :headers => {})
    client = TempoDB::Client.new("key", "secret")
    client.get_series(:keys => "my_key")
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
    context "with no options provided" do
      it "lists all series in the database" do
        stub_request(:get, "https://api.tempo-db.com/v1/series/?").
          to_return(:status => 200, :body => response_fixture('list_all_series.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        client.get_series.size.should == 7
      end
    end

    context "with filter options provided" do
      it "lists all series that meet the filtered criteria" do
        stub_request(:get, "https://api.tempo-db.com/v1/series/?key=key1&key=key2").
          to_return(:status => 200, :body => response_fixture('list_filtered_series.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        series = client.get_series(:keys => ["key1", "key2"])
        series.map(&:key).should == ["key1", "key2"]
      end
    end
  end

  describe "read_key" do
    it "has an array of DataPoints" do
      start = Time.parse("2012-01-01 00:00 UTC")
      stop = Time.parse("2012-01-02 00:00 UTC")
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/key1/data/?end=2012-01-02T00:00:00.000Z&function=&interval=&start=2012-01-01T00:00:00.000Z&tz=").
        to_return(:status => 200, :body => response_fixture('read_id_and_key.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      set = client.read_key("key1", start, stop)
      set.data.all? { |d| d.is_a?(TempoDB::DataPoint) }.should be_true
      set.data.size.should == 1440
    end

    context "with a series that does not exist" do
      it "throws a TempoDBClientError exception" do
        start = Time.parse("2012-01-01 00:00 UTC")
        stop = Time.parse("2012-01-02 00:00 UTC")
        stub_request(:get, "https://api.tempo-db.com/v1/series/key/non-existent/data/?end=2012-01-02T00:00:00.000Z&function=&interval=&start=2012-01-01T00:00:00.000Z&tz=").
          to_return(:status => 403, :body => "", :headers => {})
        client = TempoDB::Client.new("key", "secret")
        lambda { client.read_key("non-existent", start, stop) }.should raise_error(TempoDB::TempoDBClientError)
      end
    end
  end

  describe "read_id" do
    it "has an array of DataPoints" do
      start = Time.parse("2012-01-01 00:00 UTC")
      stop = Time.parse("2012-01-02 00:00 UTC")
      stub_request(:get, "https://api.tempo-db.com/v1/series/id/3c9b4f3a19114a7eb670ff7c4917f315/data/?end=2012-01-02T00:00:00.000Z&function=&interval=&start=2012-01-01T00:00:00.000Z&tz=").
        to_return(:status => 200, :body => response_fixture('read_id_and_key.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      set = client.read_id("3c9b4f3a19114a7eb670ff7c4917f315", start, stop)
      set.data.all? { |d| d.is_a?(TempoDB::DataPoint) }.should be_true
      set.data.size.should == 1440
    end
  end

  describe "write_id" do
    it "adds data points to the specific series id" do
      stub_request(:post, "https://api.tempo-db.com/v1/series/id/0e3178aea7964c4cb1a15db1e80e2a7f/data/").
        to_return(:status => 200, :body => "", :headers => {})
      points = [
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 0, 0), 12.34),
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 1, 0), 1.874),
              TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 2, 0), 21.52)
             ]
      client = TempoDB::Client.new("key", "secret")
      client.write_id("0e3178aea7964c4cb1a15db1e80e2a7f", points).should == {}
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
      client.write_key("key3", points).should == {}
    end
  end

  describe "write_bulk" do
    it "writes multiple values to multiple series at the same timestamp" do
      stub_request(:post, "https://api.tempo-db.com/v1/data/").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      ts = Time.utc(2012, 1, 8, 1, 21)
      data = [
              { :id => '0e3178aea7964c4cb1a15db1e80e2a7f', :v => 4.164 },
              { :key => 'key3', :v => 324.991 }
             ]
      client.write_bulk(ts, data).should == {}
    end

    context "with some series that exist and others that don't" do
      it "creates the non-existent series and writes to all series" do
        stub_request(:post, "https://api.tempo-db.com/v1/data/").
          to_return(:status => 200, :body => "", :headers => {})
        client = TempoDB::Client.new("key", "secret")
        ts = Time.utc(2012, 1, 8, 1, 21)
        data = [
                { :id => 'non-existent', :v => 4.164 },
                { :key => 'key3', :v => 324.991 }
               ]
        client.write_bulk(ts, data).should == {}
      end
    end
  end

  describe "increment_id" do
    it "adds the value to the datapoint value for the given series id" do
      stub_request(:post, "https://api.tempo-db.com/v1/series/id/0e3178aea7964c4cb1a15db1e80e2a7f/increment/").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      points = [
                TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 0, 0), 1),
                TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 1, 0), 2),
                TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 2, 0), 1)
               ]
      client.increment_id("0e3178aea7964c4cb1a15db1e80e2a7f", points).should == {}
    end
  end

  describe "increment_key" do
    it "adds the value to the datapoint value for the given series key" do
      stub_request(:post, "https://api.tempo-db.com/v1/series/key/key3/increment/").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      points = [
                TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 0, 0), 1),
                TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 1, 0), 2),
                TempoDB::DataPoint.new(Time.utc(2012, 1, 1, 1, 2, 0), 1)
               ]
      client.increment_key("key3", points).should == {}
    end
  end

  describe "increment_bulk" do
    it "increments multiple series at the same timestamp with multiple values" do
      stub_request(:post, "https://api.tempo-db.com/v1/increment/").
       to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      ts = Time.utc(2012, 1, 8, 1, 21)
      data = [
              { :id => '0e3178aea7964c4cb1a15db1e80e2a7f', :v => 4.164 },
              { :key => 'key3', :v => 324.991 }
             ]
      client.increment_bulk(ts, data).should == {}
    end
  end

  describe "delete_id" do
    it "deletes a single point when the start and stop are the same time" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/id/38268c3b231f1266a392931e15e99231/data/?end=2012-01-01T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      time = Time.utc(2012, 1, 1)
      client.delete_id("38268c3b231f1266a392931e15e99231", time, time).should == {}
    end

    it "deletes a range of points between the start and stop" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/id/38268c3b231f1266a392931e15e99231/data/?end=2012-01-02T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      start = Time.utc(2012, 1, 1)
      stop = Time.utc(2012, 1, 2)
      client.delete_id("38268c3b231f1266a392931e15e99231", start, stop).should == {}
    end
  end

  describe "delete_key" do
    it "deletes a single point when the start and stop are the same time" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/key/key3/data/?end=2012-01-01T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      time = Time.utc(2012, 1, 1)
      client.delete_key("key3", time, time).should == {}
    end

    it "deletes a range of points between the start and stop" do
      stub_request(:delete, "https://api.tempo-db.com/v1/series/key/key3/data/?end=2012-01-02T00:00:00.000Z&start=2012-01-01T00:00:00.000Z").
        to_return(:status => 200, :body => "", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      start = Time.utc(2012, 1, 1)
      stop = Time.utc(2012, 1, 2)
      client.delete_key("key3", start, stop).should == {}
    end
  end
end
