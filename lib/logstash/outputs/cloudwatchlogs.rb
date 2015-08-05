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

require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"

require "time"

# This output lets you send log data to AWS CloudWatch Logs service
#
class LogStash::Outputs::CloudWatchLogs < LogStash::Outputs::Base

  include LogStash::PluginMixins::AwsConfig::V2

  config_name "cloudwatchlogs"

  # Constants
  LOG_GROUP_NAME = "log_group_name"
  LOG_STREAM_NAME = "log_stream_name"
  SEQUENCE_TOKEN = "sequence_token"
  TIMESTAMP = "@timestamp"
  MESSAGE = "message"

  PER_EVENT_OVERHEAD = 26
  MAX_BATCH_SIZE = 1024 * 1024
  MAX_BATCH_COUNT = 10000
  MAX_DISTANCE_BETWEEN_EVENTS = 86400 * 1000
  MIN_DELAY = 0.2
  MIN_BUFFER_DURATION = 5000
  # Backoff up to 64 seconds upon failure
  MAX_BACKOFF_IN_SECOND = 64

  # The destination log group
  config :log_group_name, :validate => :string, :required => true

  # The destination log stream
  config :log_stream_name, :validate => :string, :required => true

  # The max number of log events in a batch.
  config :batch_count, :validate => :number, :default => MAX_BATCH_COUNT

  # The max size of log events in a batch.
  config :batch_size, :validate => :number, :default => MAX_BATCH_SIZE

  # The amount of time to batch log events, in milliseconds.
  config :buffer_duration, :validate => :number, :default => 5000

  # The max number of batches to buffer.
  # Log event is added to batch first. When batch is full or exceeds specified
  # buffer_duration milliseconds, batch is added to queue which is consumed by
  # a different thread. When both batch and queue are full, the add operation is
  # blocked.
  config :queue_size, :validate => :number, :default => 5

  # Print out the log events to stdout
  config :dry_run, :validate => :boolean, :default => false

  attr_accessor :sequence_token, :last_flush, :cwl

  # Only accessed by tests
  attr_reader :buffer

  public
  def register
    require "aws-sdk"
    @cwl = Aws::CloudWatchLogs::Client.new(aws_options_hash)

    if @batch_count > MAX_BATCH_COUNT
      @logger.warn(":batch_count exceeds the max number of log events. Use #{MAX_BATCH_COUNT} instead.")
      @batch_count = MAX_BATCH_COUNT
    end
    if @batch_size > MAX_BATCH_SIZE
      @logger.warn(":batch_size exceeds the max size of log events. Use #{MAX_BATCH_SIZE} instead.")
      @batch_size = MAX_BATCH_SIZE
    end
    if @buffer_duration < MIN_BUFFER_DURATION
      @logger.warn(":buffer_duration is smaller than the min value. Use #{MIN_BUFFER_DURATION} instead.")
      @buffer_duration = MIN_BUFFER_DURATION
    end
    @sequence_token = nil
    @last_flush = Time.now.to_f
    @buffer = Buffer.new(
      max_batch_count: batch_count, max_batch_size: batch_size,
      buffer_duration: @buffer_duration, out_queue_size: @queue_size, logger: @logger,
      size_of_item_proc: Proc.new {|event| event[:message].bytesize + PER_EVENT_OVERHEAD})
    @publisher = Thread.new do
      @buffer.deq do |batch|
        flush(batch)
      end
    end
  end # def register

  public
  def receive(event)
    return unless output?(event)

    if event == LogStash::SHUTDOWN
      @buffer.close
      @publisher.join
      @logger.info("CloudWatch Logs output plugin shutdown.")
      finished
      return
    end
    return if invalid?(event)

    @buffer.enq(
      {:timestamp => event.timestamp.time.to_f*1000,
       :message => event[MESSAGE] })
  end # def receive

  public
  def teardown
    @logger.info("Going to clean up resources")
    @buffer.close
    @publisher.join
    @cwl = nil
    finished
  end # def teardown

  public
  def flush(events)
    return if events.nil? or events.empty?
    log_event_batches = prepare_log_events(events)
    log_event_batches.each do |log_events|
      put_log_events(log_events)
    end
  end

  private
  def put_log_events(log_events)
    return if log_events.nil? or log_events.empty?
    # Shouldn't send two requests within MIN_DELAY
    delay = MIN_DELAY - (Time.now.to_f - @last_flush)
    sleep(delay) if delay > 0
    backoff = 1
    begin
      @logger.info("Sending #{log_events.size} events to #{@log_group_name}/#{@log_stream_name}")
      @last_flush = Time.now.to_f
      if @dry_run
        log_events.each do |event|
          puts event[:message]
        end
        return
      end
      response = @cwl.put_log_events(
          :log_group_name => @log_group_name,
          :log_stream_name => @log_stream_name,
          :log_events => log_events,
          :sequence_token => @sequence_token
      )
      @sequence_token = response.next_sequence_token
    rescue Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException => e
      @logger.warn(e)
      if /sequenceToken(?:\sis)?: ([^\s]+)/ =~ e.to_s
        if $1 == 'null'
          @sequence_token = nil
        else
          @sequence_token = $1
        end
        @logger.info("Will retry with new sequence token #{@sequence_token}")
        retry
      else
        @logger.error("Cannot find sequence token from response")
      end
    rescue Aws::CloudWatchLogs::Errors::DataAlreadyAcceptedException => e
      @logger.warn(e)
      if /sequenceToken(?:\sis)?: ([^\s]+)/ =~ e.to_s
        if $1 == 'null'
          @sequence_token = nil
        else
          @sequence_token = $1
        end
        @logger.info("Data already accepted and no need to resend")
      else
        @logger.error("Cannot find sequence token from response")
      end
    rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException => e
      @logger.info("Will create log group/stream and retry")
      begin
        @cwl.create_log_group(:log_group_name => @log_group_name)
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
        @logger.info("Log group #{@log_group_name} already exists")
      rescue Exception => e
        @logger.error(e)
      end
      begin
        @cwl.create_log_stream(:log_group_name => @log_group_name, :log_stream_name => @log_stream_name)
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException => e
        @logger.info("Log stream #{@log_stream_name} already exists")
      rescue Exception => e
        @logger.error(e)
      end
      retry
    rescue Aws::CloudWatchLogs::Errors::InvalidParameterException => e
      # swallow exception
      @logger.error("Skip batch due to #{e}")
    rescue Exception => e
      if backoff * 2 <= MAX_BACKOFF_IN_SECOND
        backoff = backoff * 2
      end
      @logger.error("Will retry for #{e} after #{backoff} seconds")
      sleep backoff
      retry
    end
  end# def flush

  private
  def invalid?(event)
    status = event[TIMESTAMP].nil? || event[MESSAGE].nil?
    if status
      @logger.warn("Skipping invalid event #{event.to_hash}")
    end
    return status
  end

  private
  def prepare_log_events(events)
    log_events = events.sort {|e1,e2| e1[:timestamp] <=> e2[:timestamp]}
    batches = []
    if log_events[-1][:timestamp] - log_events[0][:timestamp] > MAX_DISTANCE_BETWEEN_EVENTS
      temp_batch = []
      log_events.each do |log_event|
        if temp_batch.empty? || log_event[:timestamp] - temp_batch[0][:timestamp] <= MAX_DISTANCE_BETWEEN_EVENTS
          temp_batch << log_event
        else
          batches << temp_batch
          temp_batch = []
          temp_batch << log_event
        end
      end
      if not temp_batch.empty?
        batches << temp_batch
      end
    else
      batches << log_events
    end
    batches
  end

  ##
  # This class buffers series of single item to batches and puts batches to
  # a SizedQueue for consumption.
  # A buffer includes an ongoing batch and an out queue. An item is added
  # to the ongoing batch first, when the ongoing batch becomes to ready for
  # consumption and then is added to out queue/emptified.
  # An ongoing batch becomes to comsumption ready if the number of items is going to
  # exceed *max_batch_count*, or the size of items is going to exceed
  # *max_batch_size*, with the addition of one more item,
  # or the batch has opend more than *buffer_duration* milliseconds and has at least one item.

  class Buffer

    CLOSE_BATCH = :close

    attr_reader :in_batch, :in_count, :in_size, :out_queue

    # Creates a new buffer
    def initialize(options = {})
      @max_batch_count = options.fetch(:max_batch_count)
      @max_batch_size = options.fetch(:max_batch_size)
      @buffer_duration = options.fetch(:buffer_duration)
      @out_queue_size = options.fetch(:out_queue_size, 10)
      @logger = options.fetch(:logger, nil)
      @size_of_item_proc = options.fetch(:size_of_item_proc)
      @in_batch = Array.new
      @in_count = 0
      @in_size = 0
      @out_queue = SizedQueue.new(@out_queue_size)
      @batch_update_mutex = Mutex.new
      @last_batch_time = Time.now
      if @buffer_duration > 0
        @scheduled_batcher = Thread.new do
          loop do
            sleep(@buffer_duration / 1000.0)
            enq(:scheduled)
          end
        end
      end
    end

    # Enques an item to buffer
    #
    # * If ongoing batch is not full with this addition, adds item to batch.
    # * If ongoing batch is full with this addition, adds item to batch and add batch to out queue.
    # * If ongoing batch is going to overflow with this addition, adds batch to out queue,
    # and then adds the item to the new batch
    def enq(item)
      @batch_update_mutex.synchronize do
        if item == :scheduled || item == :close
          add_current_batch_to_out_queue(item)
          return
        end
        status = try_add_item(item)
        if status != 0
          add_current_batch_to_out_queue(:add)
          if status == -1
            try_add_item(item)
          end
        end
      end
    end

    # Closes the buffer
    #
    # Adds current batch to the queue and adds CLOSE_BATCH to queue.
    # Waits until consumer completes.
    def close
      while @in_size != 0 do
        enq(:close)
        sleep(1)
      end
      @out_queue.enq(CLOSE_BATCH)
    end

    # Deques ready for consumption batches
    #
    # The caller blocks on this call until the buffer is closed.
    def deq(&proc)
      loop do
        batch = @out_queue.deq
        if batch == CLOSE_BATCH
          break
        end
        proc.call(batch)
      end
    end

    private
    # Tries to add an item to buffer
    def try_add_item(item)
      item_size = @size_of_item_proc.call(item)
      if @in_count + 1 == @max_batch_count ||
        @in_size + item_size == @max_batch_size
        # accept item, but can't accept more items
        add_item(item)
        return 1
      elsif @in_size + item_size > @max_batch_size
        # cannot accept item
        return -1
      else
        add_item(item)
        # accept item, and may accept next item
        return 0
      end
    end

    # Adds item to batch
    def add_item(item)
      @in_batch << item
      @in_count += 1
      @in_size += @size_of_item_proc.call(item)
    end

    # Adds batch to out queue
    def add_current_batch_to_out_queue(from)
      if from == :scheduled && (Time.now - @last_batch_time) * 1000 < @buffer_duration
        return
      end
      if @in_batch.size == 0
        @last_batch_time = Time.now
        return
      end
      @logger.debug("Added batch with #{in_count} items in #{in_size} by #{from}") if @logger
      @out_queue.enq(@in_batch)
      @in_batch = Array.new
      @in_count = 0
      @in_size = 0
      @last_batch_time = Time.now
    end

  end

end # class LogStash::Outputs::CloudWatchLogs
