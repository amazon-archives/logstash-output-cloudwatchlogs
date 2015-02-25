# logstash-output-cloudwatchlogs
A logstash plugin that allows to send logs to AWS CloudWatch Logs service.

## Developing

### 1. Plugin Developement and Testing

#### Code
- To get started, you'll need JRuby with the Bundler gem installed.

- Clone the repository.

- Install dependencies
```sh
bundle install
```

#### Test

- Update your dependencies

```sh
bundle install
```

- Run tests

```sh
bundle exec rspec
```

### 2. Running your unpublished Plugin in Logstash

#### 2.1 Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the local plugin path, for example:
```ruby
gem "logstash-output-cloudwatchlogs", :path => "/your/local/logstash-output-cloudwatchlogs"
```
- Install plugin
```sh
bin/plugin install --no-verify
```
- Run Logstash with your plugin

At this point any modifications to the plugin code will be applied to this local Logstash setup. After modifying the plugin, simply rerun Logstash.

#### 2.2 Run in an installed Logstash

You can use the same **2.1** method to run your plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to your local plugin development directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-output-cloudwatchlogs.gemspec
```
- Install the plugin from the Logstash home
```sh
bin/plugin install /your/local/plugin/logstash-output-cloudwatchlogs.gem
```
- Start Logstash and proceed to test the plugin

## Usage

Below sample configuration reads 2 log4j logs and sends them to 2 log streams respectively.

```
input {
  file {
    path => "/path/to/app1.log"
    start_position => beginning
    tags => ["app1"]
  }
  file {
    path => "/path/to/app2.log"
    start_position => beginning
    tags => ["app2"]
  }
}

filter {
  multiline {
    pattern => "^%{MONTHDAY} %{MONTH} %{YEAR} %{TIME}"
    negate => true
    what => "previous"
  }
  grok {
    match => { "message" => "(?<timestamp>%{MONTHDAY} %{MONTH} %{YEAR} %{TIME})" }
  }
  date {
    match => [ "timestamp", "dd MMM yyyy HH:mm:ss,SSS" ]
    target => "@timestamp"
  }
}

output {
  if "app1" in [tags] {
    cloudwatchlogs {
      "log_group_name" => "app1"
      "log_stream_name" => "host1"
    }
  }
  if "app2" in [tags] {
    cloudwatchlogs {
      "log_group_name" => "app2"
      "log_stream_name" => "host1"
    }
  }
}

```

Here are all the supported options:

* region: string, the AWS region, defaults to us-east-1.
* access_key_id: string, specifies the access key.
* secret_access_key: string, specifies the secret access key.
* aws_credentials_file: string, points to a file which specifies access_key_id and secret_access_key.
* log_group_name: string, required, specifies the destination log group. A log group will be created automatically if it doesn't already exist.
* log_stream_name: string, required, specifies the destination log stream. A log stream will be created automatically if it doesn't already exist.
* batch_count: number, the max number of log events in a batch, up to 10000.
* batch_size: number, the max size of log events in a batch, in bytes, up to 1048576.
* buffer_duration: number, the amount of time to batch log events, in milliseconds, from 5000 milliseconds.
* queue_size: number, the max number of batches to buffer, defaults to 5.
* dry_run: boolean, prints out the log events to stdout instead of sending them to CloudWatch Logs service.


In addition to configuring the AWS credential in the configuration file, credentials can also be loaded automatically from the following locations:

* ENV['AWS_ACCESS_KEY_ID'] and ENV['AWS_SECRET_ACCESS_KEY']
* The shared credentials ini file at ~/.aws/credentials (more information)
* From an instance profile when running on EC2

```
cloudwatchlogs {
  "log_group_name" => "lg2"
  "log_stream_name" => "ls1"
  "batch_count" => 1000
  "batch_size" => 1048576
  "buffer_duration" => 5000
  "queue_size" => 10
  "dry_run" => false
}
```
## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
