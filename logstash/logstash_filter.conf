input {
  beats {
    port => 5044
    codec => json
  }
  
  tcp {
    port => 5000
    codec => json_lines
  }
  
  # Metric Collector 입력
  http {
    port => 8080
    codec => json
  }
}

filter {
  # 타임스탬프 파싱
  date {
    match => [ "timestamp", "ISO8601", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss" ]
    target => "@timestamp"
  }
  
  # 메트릭 타입별 처리
  if [metric_type] == "system" {
    mutate {
      add_field => { 
        "metric_category" => "infrastructure"
        "index_suffix" => "system"
      }
    }
    
    # CPU 메트릭 계산
    if [cpu_idle] {
      ruby {
        code => "event.set('cpu_usage', 100 - event.get('cpu_idle').to_f)"
      }
    }
  }
  
  if [metric_type] == "application" {
    mutate {
      add_field => { 
        "metric_category" => "application"
        "index_suffix" => "app"
      }
    }
    
    # 응답 시간 분류
    if [response_time] {
      if [response_time] > 5000 {
        mutate { add_tag => [ "slow_response" ] }
      } else if [response_time] > 1000 {
        mutate { add_tag => [ "moderate_response" ] }
      } else {
        mutate { add_tag => [ "fast_response" ] }
      }
    }
  }
  
  if [metric_type] == "database" {
    mutate {
      add_field => { 
        "metric_category" => "database"
        "index_suffix" => "db"
      }
    }
    
    # 쿼리 성능 분석
    if [query_time] and [query_time] > 1000 {
      mutate { 
        add_tag => [ "slow_query" ]
        add_field => { "alert_required" => "true" }
      }
    }
  }
  
  # 호스트 정보 추가
  if ![hostname] {
    mutate {
      add_field => { "hostname" => "%{host}" }
    }
  }
  
  # 환경 구분
  if [hostname] =~ /^dev-/ {
    mutate { add_field => { "environment" => "development" } }
  } else if [hostname] =~ /^stg-/ {
    mutate { add_field => { "environment" => "staging" } }
  } else if [hostname] =~ /^prod-/ {
    mutate { add_field => { "environment" => "production" } }
  } else {
    mutate { add_field => { "environment" => "unknown" } }
  }
  
  # 불필요한 필드 제거
  mutate {
    remove_field => [ "host", "port", "@version", "headers" ]
  }
  
  # 데이터 검증
  if ![metric_type] or ![timestamp] {
    mutate {
      add_tag => [ "_invalid_metric" ]
    }
  }
  
  # 로그 파일에 기록 (디버깅용)
  if "_invalid_metric" not in [tags] {
    file {
      path => "/home/logstash/logstash-8.1.3_metric/logstash_rx_message.log"
      codec => line { format => "%{@timestamp} - %{metric_type} - %{hostname}" }
    }
  }
}

output {
  # Kafka로 전송
  if "_invalid_metric" not in [tags] {
    kafka {
      bootstrap_servers => "kafka1:9092,kafka2:9092,kafka3:9092"
      topic_id => "metrics-%{[index_suffix]}"
      codec => json
      acks => "1"
      retries => 3
      batch_size => 100
      linger_ms => 10
    }
  }
  
  # 디버깅용 stdout
  if "_debug" in [tags] {
    stdout {
      codec => rubydebug
    }
  }
  
  # 에러 메트릭은 별도 처리
  if "_invalid_metric" in [tags] {
    file {
      path => "/var/log/logstash/invalid_metrics.log"
      codec => json
    }
  }
}