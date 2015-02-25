Gem::Specification.new do |s|

  s.name            = 'logstash-output-cloudwatchlogs'
  s.version         = '0.9.0'
  s.licenses        = ['Amazon Software License']
  s.summary         = "This output lets you send logs to AWS CloudWatch Logs service"
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["AWS"]
  s.email           = 'cloudwatch-logs-feedback@amazon.com'
  s.homepage        = "http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchLogs.html"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core', '>= 1.5.0', '< 2.0.0'
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'logstash-mixin-aws', '>= 1.0.0'
  s.add_runtime_dependency 'aws-sdk', ['~> 2']

  s.add_development_dependency 'logstash-devutils'
end
