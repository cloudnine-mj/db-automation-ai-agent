# 데이터 파이프라인 자동화 AI Agent

## 📋 프로젝트 개요

ELK 스택 기반 데이터 파이프라인의 운영 자동화를 위한 지능형 AI Agent 시스템입니다. OpenSearch, PostgreSQL, Kafka, Logstash 등의 컴포넌트를 모니터링하고 자동으로 관리합니다.

## 🚀 주요 기능

### 1. 실시간 모니터링

- **OpenSearch**: 클러스터 상태, 인덱스 상태, 샤드 할당 모니터링
- **PostgreSQL**: Active/Standby 복제 상태, 느린 쿼리 감지
- **Kafka**: 브로커 상태, 컨슈머 랙 모니터링
- **Logstash**: 프로세스 상태, CPU 사용률 모니터링

### 2. 자동화 기능

- **자동 복구**: 서비스 장애 시 자동 재시작
- **로그 관리**: logstash_rx_message.log 자동 정리
- **인덱스 관리**: 30일 이상 오래된 인덱스 자동 삭제
- **성능 최적화**: VACUUM ANALYZE 자동 실행
- **페일오버**: PostgreSQL 자동 페일오버

### 3. AI 기반 기능

- **이상 탐지**: Isolation Forest 알고리즘으로 이상 패턴 감지
- **성능 예측**: Random Forest로 미래 리소스 사용량 예측
- **지능형 최적화**: 메트릭 기반 자동 최적화 제안
- **예측 유지보수**: 장애 발생 확률 예측 및 유지보수 일정 생성

### 4. 알림 시스템

- 이메일 알림
- Slack 웹훅 연동
- 임계치 기반 알림 (CPU, Memory, Disk)

## 🏗️ 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                     AI Monitoring Agent                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Anomaly  │  │Performance│  │Optimizer │  │Predictor │  │
│  │ Detector │  │  Monitor  │  │  Module  │  │  Module  │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
└─────────────────────────┬───────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼────────┐ ┌──────▼──────┐ ┌───────▼────────┐
│   OpenSearch   │ │ PostgreSQL  │ │     Kafka      │
│  ┌──────────┐  │ │ ┌─────────┐ │ │  ┌──────────┐  │
│  │  Master  │  │ │ │ Active  │ │ │  │ Broker 1 │  │
│  └──────────┘  │ │ └─────────┘ │ │  └──────────┘  │
│  ┌──────────┐  │ │ ┌─────────┐ │ │  ┌──────────┐  │
│  │  Data 1  │  │ │ │ Standby │ │ │  │ Broker 2 │  │
│  └──────────┘  │ │ └─────────┘ │ │  └──────────┘  │
│  ┌──────────┐  │ │             │ │  ┌──────────┐  │
│  │  Data 2  │  │ │             │ │  │ Broker 3 │  │
│  └──────────┘  │ │             │ │  └──────────┘  │
└────────────────┘ └─────────────┘ └────────────────┘
        ▲                                    │
        │                                    │
┌───────┴────────┐                  ┌───────▼────────┐
│   Logstash     │                  │   Logstash     │
│   Indexer      │◄─────────────────│    Filter      │
│  (3 nodes)     │                  │   (3 nodes)    │
└────────────────┘                  └────────────────┘
```

## 📦 설치 방법

### 1. 요구사항

- Python 3.8+
- Docker & Docker Compose (개발환경용)
- PostgreSQL 클라이언트
- SSH 접근 권한 (원격 서버 관리용)

### 2. 패키지 설치

```bash
pip install -r requirements.txt
```

### 3. 설정 파일 수정

```bash
cp config.json.example config.json
# config.json 파일을 환경에 맞게 수정
```

### 4. Agent 실행

```bash
# 단독 실행
python data_pipeline_agent.py

# 백그라운드 실행
nohup python data_pipeline_agent.py > agent.log 2>&1 &

# systemd 서비스로 등록
sudo cp pipeline-agent.service /etc/systemd/system/
sudo systemctl enable pipeline-agent
sudo systemctl start pipeline-agent
```

## 🐳 Docker로 개발환경 구축

```bash
# 전체 스택 실행
docker-compose up -d

# 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs -f pipeline-agent

# 중지
docker-compose down
```

## 🛠️ CLI 유틸리티 사용법

### 헬스 체크

```bash
python automation_utilities.py health all
python automation_utilities.py health opensearch
```

### 백업

```bash
python automation_utilities.py backup all --dir /backup
python automation_utilities.py backup postgresql --dir /backup
```

### 복원

```bash
python automation_utilities.py restore postgresql /backup/postgresql_20240101_120000.tar.gz
```

### 성능 튜닝

```bash
python automation_utilities.py tune all
python automation_utilities.py tune opensearch
```

### 스케일링

```bash
python automation_utilities.py scale logstash up --count 2
python automation_utilities.py scale logstash down --count 1
```

### 페일오버

```bash
python automation_utilities.py failover postgresql postgres-standby.example.com
```

## 📊 모니터링 대시보드

OpenSearch Dashboard에 접속하여 시각화된 메트릭을 확인할 수 있습니다:

- URL: http://localhost:5601
- 기본 인덱스 패턴: `logstash-*`

## 🔧 주요 설정

### config.json 구조

```json
{
  "opensearch": {
    "master_host": "opensearch-master.example.com",
    "data_nodes": ["node1", "node2"],
    "port": 9200
  },
  "postgresql": {
    "active": {...},
    "standby": {...}
  },
  "kafka": {
    "bootstrap_servers": ["kafka1:9092", "kafka2:9092", "kafka3:9092"]
  },
  "logstash": {
    "filter_servers": [...],
    "indexer_servers": [...]
  },
  "alerts": {
    "email_enabled": true,
    "email_recipients": ["admin@example.com"],
    "alert_threshold_cpu": 80.0
  }
}
```

## 🚨 알림 임계치

```
메트릭경고 (Warning)위험 (Critical)
CPU 사용률70%90%
메모리 사용률75%85%
디스크 사용률80%90%
쿼리 응답시간1000ms5000ms
컨슈머 랙100010000
```

## 📈 AI 모델 학습

### 이상 탐지 모델 학습

```python
from ai_monitoring_module import AnomalyDetector
import pandas as pd

detector = AnomalyDetector()
historical_data = pd.read_csv('metrics_history.csv')
detector.train(historical_data)
detector.save_model('models/anomaly_detector.pkl')
```

### 성능 예측 모델 학습

```python
from ai_monitoring_module import PerformancePredictor
import pandas as pd

predictor = PerformancePredictor()
historical_data = pd.read_csv('performance_history.csv')
predictor.train(historical_data)
```

## 🔍 트러블슈팅

### 1. Logstash CPU 사용률 높음

```bash
# 자동 해결
python automation_utilities.py tune logstash

# 수동 해결
cat /dev/null > /home/logstash/logstash-8.1.3_metric/logstash_rx_message.log
systemctl restart logstash
```

### 2. OpenSearch 인덱스 문제

```bash
# 샤드 재할당
curl -X POST "localhost:9200/_cluster/reroute?retry_failed=true"

# 인덱스 최적화
curl -X POST "localhost:9200/_forcemerge?max_num_segments=1"
```

### 3. PostgreSQL 복제 지연

```sql
-- Active 서버에서 실행
SELECT client_addr, state, sync_state, 
       pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) as lag_bytes
FROM pg_stat_replication;
```

### 4. Kafka 컨슈머 랙

```bash
# 컨슈머 그룹 상태 확인
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups

# 오프셋 리셋
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group --reset-offsets --to-latest --execute --all-topics
```

## 📝 로그 위치

- Agent 로그: `/var/log/data_pipeline_agent.log`
- OpenSearch: `/var/log/opensearch/`
- PostgreSQL: `/var/log/postgresql/`
- Kafka: `/var/log/kafka/`
- Logstash: `/var/log/logstash/`