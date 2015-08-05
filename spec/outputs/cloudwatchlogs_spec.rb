# encoding: utf-8

#
#  Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Amazon Software License (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#
#  http://aws.amazon.com/asl/
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#  express or implied. See the License for the specific language governing
#  permissions and limitations under the License.

require "logstash/devutils/rspec/spec_helper"
require "logstash/plugin"
require "logstash/json"
require "logstash/timestamp"
require "logstash/outputs/cloudwatchlogs"

require "aws-sdk"

describe "outputs/cloudwatchlogs" do

  describe "#new" do
    it "should raise error when log group name is not configured" do
      expect {
        LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
          "log_group_name" => "lg")
      }.to raise_error(LogStash::ConfigurationError)
    end

    it "should raise error when log stream name is not configured" do
      expect {
        LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
          "log_stream_name" => "ls")
      }.to raise_error(LogStash::ConfigurationError)
    end

    it "should create the output with log group/stream name" do
      expect {
        LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
          "log_group_name" => "lg", "log_stream_name" => "ls")
      }.to_not raise_error
    end
  end

  describe "#register" do
    it "should set the batch_count to MAX_BATCH_COUNT if a larger value is configured" do
      output = LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
        "log_group_name" => "lg", "log_stream_name" => "ls",
        "batch_count" => LogStash::Outputs::CloudWatchLogs::MAX_BATCH_COUNT + 1)
      expect {output.register}.to_not raise_error
      output.batch_count.should eql(LogStash::Outputs::CloudWatchLogs::MAX_BATCH_COUNT)
    end

    it "should set the batch_size to MAX_BATCH_SIZE if a larger value is configured" do
      output = LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
        "log_group_name" => "lg", "log_stream_name" => "ls",
        "batch_count" => LogStash::Outputs::CloudWatchLogs::MAX_BATCH_SIZE + 1)
      expect {output.register}.to_not raise_error
      output.batch_size.should eql(LogStash::Outputs::CloudWatchLogs::MAX_BATCH_SIZE)
    end

    it "should set the buffer_duration to MIN_BUFFER_DURATION if a smaler value is configured" do
      output = LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
        "log_group_name" => "lg", "log_stream_name" => "ls",
        "buffer_duration" => LogStash::Outputs::CloudWatchLogs::MIN_BUFFER_DURATION - 1)
      expect {output.register}.to_not raise_error
      output.buffer_duration.should eql(LogStash::Outputs::CloudWatchLogs::MIN_BUFFER_DURATION)
    end
  end

  describe "#receive" do
    before :each do
      @output = LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
        "log_group_name" => "lg", "log_stream_name" => "ls")
      @output.register
    end

    context "when event is invalid" do
      before :each do
        @event = LogStash::Event.new
        @event.timestamp = LogStash::Timestamp.coerce("2015-02-13T01:19:08Z")
        @event["message"] = "test"
        expect(@output.buffer).not_to receive(:enq)
      end
      context "when event doesn't have @timestamp" do
        it "should not save the event" do
          @event.remove("@timestamp")
          expect { @output.receive(@event) }.to_not raise_error
        end
      end
      context "when event doesn't have message" do
        it "should not save the event" do
          @event.remove("message")
          expect { @output.receive(@event) }.to_not raise_error
        end
      end
    end

    context "when event is valid" do
      context "when first event is received" do
        it "should save the event to buffer" do
          expect(@output.buffer).to receive(:enq) { {:timestamp => 1423786748000.0, :message => "test"} }
          event = LogStash::Event.new
          event.timestamp = LogStash::Timestamp.coerce("2015-02-13T01:19:08Z")
          event["message"] = "test"
          expect { @output.receive(event) }.to_not raise_error
        end
      end

    end
  end

  describe "#flush" do
    before :each do
      @cwl = Aws::CloudWatchLogs::Client.new(:region => "us-east-1")
      @output = LogStash::Plugin.lookup("output", "cloudwatchlogs").new(
        "log_group_name" => "lg", "log_stream_name" => "ls")
      @output.register
      @output.cwl = @cwl
    end

    context "when received zero events" do
      it "should not make the service call" do
        expect(@cwl).not_to receive(:put_log_events)
        @output.flush([])
      end
    end

    context "when received some events" do
      before :each do
        @output.sequence_token = 'token'
        @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
        allow(@cwl).to receive(:put_log_events) { @response }
      end
      context "when events are not sorted" do
        it "should sort the events before sending them to service" do
          expect(@cwl).to receive(:put_log_events).with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'},
              {:timestamp => 124, :message => 'zzz'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => 'token'
          ) { @response }
          @output.flush([
            {:timestamp => 124, :message => 'zzz'},
            {:timestamp => 123, :message => 'abc'}])
        end
      end
      context "when events are sorted" do
        it "should send the events as is" do
          expect(@cwl).to receive(:put_log_events).with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'},
              {:timestamp => 124, :message => 'zzz'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => 'token'
          ) { @response }
          @output.flush([
            {:timestamp => 123, :message => 'abc'},
            {:timestamp => 124, :message => 'zzz'}])
        end
      end
      context "when log events span more than 24 hours" do
        it "should break log events into multiple batches and no batch spans more than 24 hours" do
          twenty_four_hours_in_mills = 24 * 60 * 60 * 1000
          expect(@cwl).to receive(:put_log_events).once.with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => 'token'
          ) { @response }
          expect(@cwl).to receive(:put_log_events).once.with(
            :log_events => [
              {:timestamp => 123 + twenty_four_hours_in_mills + 1, :message => 'zzz'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => 'ntoken'
          )
          @output.flush([
            {:timestamp => 123, :message => 'abc'},
            {:timestamp => 123 + twenty_four_hours_in_mills + 1, :message => 'zzz'}])
        end
      end
      context "when log events span exactly 24 hours" do
        it "should not break log events into multiple batches" do
          twenty_four_hours_in_mills = 24 * 60 * 60 * 1000
          expect(@cwl).to receive(:put_log_events).once.with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'},
              {:timestamp => 123 + twenty_four_hours_in_mills, :message => 'zzz'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => 'token'
          ) { @response }
          @output.flush([
            {:timestamp => 123, :message => 'abc'},
            {:timestamp => 123 + twenty_four_hours_in_mills, :message => 'zzz'}])
        end
      end
    end

    describe "error handling" do
      context "when sending the first batch" do
        it "should use null sequence token" do
          @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
          allow(@cwl).to receive(:put_log_events) { @response }
          expect(@cwl).to receive(:put_log_events).with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => nil
          ) { @response }
          @output.flush([
            {:timestamp => 123, :message => 'abc'}])
        end
        context "when the log stream doesn't exist" do
          it "should create log group and log stream" do
            @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
            # throw exception for 1st call and good for following calls
            allow(@cwl).to receive(:put_log_events) do
              allow(@cwl).to receive(:put_log_events) { @response }
              raise Aws::CloudWatchLogs::Errors::ResourceNotFoundException.new(nil, nil)
            end
            expect(@cwl).to receive(:put_log_events).exactly(2).times.with(
              :log_events => [
                {:timestamp => 123, :message => 'abc'}],
              :log_group_name => 'lg',
              :log_stream_name => 'ls',
              :sequence_token => nil
            )
            expect(@cwl).to receive(:create_log_group).with(:log_group_name => 'lg')
            expect(@cwl).to receive(:create_log_stream).with(:log_group_name => 'lg', :log_stream_name => 'ls')
            @output.flush([
              {:timestamp => 123, :message => 'abc'}])
          end
        end
        context "when the log stream exists" do
          context "log stream has no log event" do
            it "should succeed" do
              @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
              allow(@cwl).to receive(:put_log_events) { @response }
              expect(@cwl).to receive(:put_log_events).with(
                :log_events => [
                  {:timestamp => 123, :message => 'abc'}],
                :log_group_name => 'lg',
                :log_stream_name => 'ls',
                :sequence_token => nil
              )
              @output.flush([
                {:timestamp => 123, :message => 'abc'}])
            end
          end

          context "log stream has some log events" do
            context "when log stream only accpeted one batch in the past" do
              context "when the sending batch is the same as accepted batch" do
                it "should not retry upon DataAlreadyAcceptedException" do
                  @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
                  @ex = Aws::CloudWatchLogs::Errors::DataAlreadyAcceptedException.new(nil, "The next batch can be sent with sequenceToken: 456")
                  # throw exception for 1st call and good for following calls
                  allow(@cwl).to receive(:put_log_events) do
                    allow(@cwl).to receive(:put_log_events) { @response }
                    raise @ex
                  end
                  expect(@cwl).to receive(:put_log_events).once.with(
                    :log_events => [
                      {:timestamp => 123, :message => 'abc'}],
                    :log_group_name => 'lg',
                    :log_stream_name => 'ls',
                    :sequence_token => nil
                  )
                  @output.flush([
                    {:timestamp => 123, :message => 'abc'}])
                end
              end
              context "when the sending batch is different than accepted batch" do
                it "should retry upon InvalidSequenceTokenException" do
                  @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
                  @ex = Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException.new(nil, "The next expected sequenceToken is: 456")
                  # throw exception for 1st call and good for following calls
                  allow(@cwl).to receive(:put_log_events) do
                    allow(@cwl).to receive(:put_log_events) { @response }
                    raise @ex
                  end
                  expect(@cwl).to receive(:put_log_events).once.with(
                    :log_events => [
                      {:timestamp => 123, :message => 'abc'}],
                    :log_group_name => 'lg',
                    :log_stream_name => 'ls',
                    :sequence_token => nil
                  )
                  expect(@cwl).to receive(:put_log_events).once.with(
                    :log_events => [
                      {:timestamp => 123, :message => 'abc'}],
                    :log_group_name => 'lg',
                    :log_stream_name => 'ls',
                    :sequence_token => '456'
                  ) { @response }
                  @output.flush([
                    {:timestamp => 123, :message => 'abc'}])
                end
              end
            end
            context "when log stream already accepted more than one batch" do
              it "should retry upon InvalidSequenceTokenException" do
                @output.sequence_token = "lasttoken"
                @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
                @ex = Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException.new(nil, "The next expected sequenceToken is: 456")
                # throw exception for 1st call and good for following calls
                allow(@cwl).to receive(:put_log_events) do
                  allow(@cwl).to receive(:put_log_events) { @response }
                  raise @ex
                end
                expect(@cwl).to receive(:put_log_events).once.with(
                  :log_events => [
                    {:timestamp => 123, :message => 'abc'}],
                  :log_group_name => 'lg',
                  :log_stream_name => 'ls',
                  :sequence_token => 'lasttoken'
                )
                expect(@cwl).to receive(:put_log_events).once.with(
                  :log_events => [
                    {:timestamp => 123, :message => 'abc'}],
                  :log_group_name => 'lg',
                  :log_stream_name => 'ls',
                  :sequence_token => '456'
                ) { @response }
                @output.flush([
                  {:timestamp => 123, :message => 'abc'}])
              end
            end
          end
        end
      end
      context "when sending batch after first batch" do
        before :each do
          @output.sequence_token = 'lasttoken'
        end
        it "should use the previous token" do
          @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
          allow(@cwl).to receive(:put_log_events) { @response }
          expect(@cwl).to receive(:put_log_events).once.with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => 'lasttoken'
          ) { @response }
          @output.flush([
            {:timestamp => 123, :message => 'abc'}])
        end
      end
      context "when sending invalid request" do
        it "should not retry" do
          @output.log_group_name = nil
          @output.log_stream_name = nil
          @output.sequence_token = nil
          @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
          @ex = Aws::CloudWatchLogs::Errors::InvalidParameterException.new(nil, nil)
          allow(@cwl).to receive(:put_log_events) { raise @ex }
          expect(@cwl).to receive(:put_log_events).once.with(
              :log_events => [
                {:timestamp => 123, :message => 'abc'}],
              :log_group_name => nil,
              :log_stream_name => nil,
              :sequence_token => nil
            )
          @output.flush([
            {:timestamp => 123, :message => 'abc'}])
        end
      end
      context "when receiving unknown exception" do
        it "should retry forever until getting back non-unknow exception" do
          @output.sequence_token = nil
          allow(@output).to receive(:sleep)
          @response = double(Aws::PageableResponse, :next_sequence_token => 'ntoken')
          @ex = Aws::CloudWatchLogs::Errors::ServiceUnavailableException.new(nil, nil)
          # Throw 7 exceptions and then return normal
          allow(@cwl).to receive(:put_log_events) do
            allow(@cwl).to receive(:put_log_events) do
              allow(@cwl).to receive(:put_log_events) do
                allow(@cwl).to receive(:put_log_events) do
                  allow(@cwl).to receive(:put_log_events) do
                    allow(@cwl).to receive(:put_log_events) do
                      allow(@cwl).to receive(:put_log_events) do
                        allow(@cwl).to receive(:put_log_events) { @response }
                        raise @ex
                      end
                      raise @ex
                    end
                    raise @ex
                  end
                  raise @ex
                end
                raise @ex
              end
              raise @ex
            end
            raise @ex
          end
          expect(@cwl).to receive(:put_log_events).exactly(8).times.with(
            :log_events => [
              {:timestamp => 123, :message => 'abc'}],
            :log_group_name => 'lg',
            :log_stream_name => 'ls',
            :sequence_token => nil
          )
          [2, 4, 8, 16, 32].each do |i|
            expect(@output).to receive(:sleep).once.with(i)
          end
          # Should sleep upto 64 seconds for each retry
          expect(@output).to receive(:sleep).twice.with(64)
          @output.flush([
            {:timestamp => 123, :message => 'abc'}])
        end
      end
    end

  end

end


describe "outputs/cloudwatchlogs/buffer" do

  describe "#initialize" do
    it "should create a buffer" do
      buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
        max_batch_count: 5, max_batch_size: 10, buffer_duration: 5000,
        out_queue_size: 5,
        size_of_item_proc: Proc.new {|item| item.bytesize})
    end
  end

  describe "#enq" do
    context "when batch is closed based on item count/size" do
      before :each do
        @buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
          max_batch_count: 5, max_batch_size: 10, buffer_duration: 0,
          out_queue_size: 5,
          size_of_item_proc: Proc.new {|item| item.bytesize})
      end

      context "when the number of items is less than max batch count" do
        it "should accept an item" do
          @buffer.enq("ab")
          @buffer.in_batch.should eql(["ab"])
          @buffer.in_size.should == 2
          @buffer.in_count.should == 1
        end
      end

      context "when the number of items is equal to the max batch size" do
        it "should batch items to the out queue and current batch is empty" do
          5.times do |i| @buffer.enq("#{i}") end
          @buffer.in_batch.should eql([])
          @buffer.out_queue.deq(true).should eql(["0", "1", "2", "3", "4"])
          @buffer.in_size.should == 0
          @buffer.in_count.should == 0
        end
      end

      context "when the number of items is greater than the max batch size" do
        it "should batch items to the out queue" do
          6.times do |i| @buffer.enq("#{i}") end
          @buffer.in_batch.should eql(["5"])
          @buffer.out_queue.deq(true).should eql(["0", "1", "2", "3", "4"])
          @buffer.in_size.should eql(1)
          @buffer.in_count.should eql(1)
        end
      end

      context "when the size of items is equal to the max batch size" do
        it "should batch items to the out queue and current batch is empty" do
          2.times do |i| @buffer.enq("abcd#{i}") end
          @buffer.in_batch.should eql([])
          @buffer.out_queue.deq(true).should eql(["abcd0", "abcd1"])
          @buffer.in_size.should == 0
          @buffer.in_count.should == 0
        end
      end

      context "when the size of items is greater than max batch size" do
        it "should batch items to the out queue" do
          3.times do |i| @buffer.enq("abc#{i}") end
          @buffer.in_batch.should eql(["abc2"])
          @buffer.out_queue.deq(true).should eql(["abc0", "abc1"])
          @buffer.in_size.should == 4
          @buffer.in_count.should == 1
        end
      end
    end

    context "when batch is closed when it passes buffer_duration milliseconds" do
      before :each do
        @buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
          max_batch_count: 5, max_batch_size: 10, buffer_duration: 1000,
          out_queue_size: 5,
          size_of_item_proc: Proc.new {|item| item.bytesize})
      end

      context "when the number of items is less than max batch count and size is less than batch size" do
        it "should batch items to the out queue" do
          @buffer.enq("ab")
          sleep(2)
          @buffer.in_batch.should eql([])
          @buffer.out_queue.deq(true).should eql(["ab"])
          @buffer.in_size.should == 0
          @buffer.in_count.should == 0
        end
      end
    end

    context "when batching is determined by size/count/duration" do
      context "when enough items are added within buffer_duration milliseconds" do
        it "should not add batch whose size or count is less than the threshold to the out queue except the first and last batch" do
          @buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
            max_batch_count: 5, max_batch_size: 100, buffer_duration: 1000,
            out_queue_size: 5,
            size_of_item_proc: Proc.new {|item| item.bytesize})
          20.times do |i|
            @buffer.enq("#{i}")
            # sleep less than buffer_duration, 5 items take 0.5 seconds to fill the buffer
            sleep(0.1)
          end
          sleep(2)
          # There will be 4 to 5 batches
          batches = []
          while !@buffer.out_queue.empty? do
            batches << @buffer.out_queue.deq(true)
          end
          batches.size.should >= 4
          batches.size.should <= 5
          batches.shift
          batches.pop
          batches.each do |batch|
            batch.size.should == 5
          end
        end
      end
    end
  end

  describe "#close" do
    it "should add last batch to out queue" do
      @buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
        max_batch_count: 5, max_batch_size: 100, buffer_duration: 1000,
        out_queue_size: 5,
        size_of_item_proc: Proc.new {|item| item.bytesize})
      consumer = Thread.new do
        @buffer.deq {}
      end
      33.times do |i|
        @buffer.enq("#{i}")
        sleep(0.01)
      end
      @buffer.close
      @buffer.in_count.should == 0
      consumer.join
    end
  end

  describe "#deq" do
    it "should keep processing items until the buffer is closed" do
      @buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
        max_batch_count: 5, max_batch_size: 100, buffer_duration: 1000,
        out_queue_size: 5,
        size_of_item_proc: Proc.new {|item| item.bytesize})
      item_count = 0
      consumer = Thread.new do
        @buffer.deq do |batch|
          item_count += batch.size
        end
      end
      33.times do |i|
        @buffer.enq("#{i}")
        sleep(0.01)
      end
      @buffer.close
      consumer.join
      item_count.should == 33
    end
  end

  it "should not miss any item" do
    @buffer = LogStash::Outputs::CloudWatchLogs::Buffer.new(
      max_batch_count: 137, max_batch_size: 1000, buffer_duration: 5000,
      out_queue_size: 50,
      size_of_item_proc: Proc.new {|item| item.bytesize})
    item_count = 0
    consumer = Thread.new do
      @buffer.deq do |batch|
        item_count += batch.size
      end
    end
    threads = []
    num_of_threads = 100
    num_of_items = 10000
    # 100 threads plus one scheduled batcher thread
    num_of_threads.times do |m|
      threads << Thread.new do
        num_of_items.times do |i|
          @buffer.enq("#{i}")
        end
      end
    end
    # let producers complete the writes
    threads.map(&:join)
    # move all items to the out queue
    @buffer.close
    # let consumer complete the read
    consumer.join
    item_count.should == num_of_items * num_of_threads
  end
end
