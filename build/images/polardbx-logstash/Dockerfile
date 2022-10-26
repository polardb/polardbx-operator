FROM jruby:9.3.8.0-jdk8 as builder
WORKDIR /workspace
ADD tools/logstash-filter-polardbx .
RUN gem install bundler:2.3.10
RUN bundle update
RUN bundle install
RUN gem build logstash-filter-polardbx.gemspec

FROM logstash:8.1.2
WORKDIR /usr/share/logstash/
USER root
COPY --from=builder /workspace/logstash-filter-polardbx-1.0.0.gem /usr/share/logstash/
RUN /usr/share/logstash/bin/logstash-plugin install /usr/share/logstash/logstash-filter-polardbx-1.0.0.gem 
RUN echo "Asia/Shanghai" > /etc/timezone

LABEL POLARX_BUSINESS=loghub