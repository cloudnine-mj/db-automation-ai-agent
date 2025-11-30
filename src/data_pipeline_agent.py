#!/usr/bin/env python3

import os
import json
import time
import logging
import asyncio
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import psycopg2
import requests
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType
import paramiko
import schedule
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/data_pipeline_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ComponentStatus(Enum):
    """컴포넌트 상태"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class ServerConfig:
    """서버 설정"""
    host: str
    port: int
    username: str
    password: Optional[str] = None
    ssh_key_path: Optional[str] = None


@dataclass
class AlertConfig:
    """알림 설정"""
    email_enabled: bool = True
    email_recipients: List[str] = None
    slack_enabled: bool = False
    slack_webhook: Optional[str] = None
    alert_threshold_cpu: float = 80.0
    alert_threshold_memory: float = 85.0
    alert_threshold_disk: float = 90.0


class OpenSearchMonitor:
    """OpenSearch 모니터링 및 관리"""
    
    def __init__(self, master_host: str, data_nodes: List[str], port: int = 9200):
        self.master_host = master_host
        self.data_nodes = data_nodes
        self.port = port
        self.base_url = f"http://{master_host}:{port}"
        
    async def check_cluster_health(self) -> Dict[str, Any]:
        """클러스터 상태 확인"""
        try:
            response = requests.get(f"{self.base_url}/_cluster/health")
            health_data = response.json()
            
            status = ComponentStatus.HEALTHY
            if health_data['status'] == 'yellow':
                status = ComponentStatus.WARNING
            elif health_data['status'] == 'red':
                status = ComponentStatus.CRITICAL
                
            return {
                'status': status,
                'cluster_status': health_data['status'],
                'number_of_nodes': health_data['number_of_nodes'],
                'active_shards': health_data['active_shards'],
                'unassigned_shards': health_data['unassigned_shards']
            }
        except Exception as e:
            logger.error(f"OpenSearch 클러스터 상태 확인 실패: {e}")
            return {'status': ComponentStatus.UNKNOWN, 'error': str(e)}
    
    async def check_index_status(self) -> List[Dict[str, Any]]:
        """인덱스 상태 확인"""
        try:
            response = requests.get(f"{self.base_url}/_cat/indices?format=json")
            indices = response.json()
            
            index_status = []
            for index in indices:
                if index['health'] != 'green':
                    index_status.append({
                        'index': index['index'],
                        'health': index['health'],
                        'docs_count': index['docs.count'],
                        'store_size': index['store.size']
                    })
            
            return index_status
        except Exception as e:
            logger.error(f"인덱스 상태 확인 실패: {e}")
            return []
    
    async def auto_delete_old_indices(self, days: int = 30) -> List[str]:
        """오래된 인덱스 자동 삭제"""
        deleted_indices = []
        try:
            response = requests.get(f"{self.base_url}/_cat/indices?format=json")
            indices = response.json()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            for index in indices:
                # 인덱스 이름에서 날짜 추출 (예: logstash-2024.01.01)
                index_name = index['index']
                if 'logstash-' in index_name:
                    try:
                        date_str = index_name.split('logstash-')[1]
                        index_date = datetime.strptime(date_str, '%Y.%m.%d')
                        
                        if index_date < cutoff_date:
                            requests.delete(f"{self.base_url}/{index_name}")
                            deleted_indices.append(index_name)
                            logger.info(f"인덱스 삭제: {index_name}")
                    except:
                        continue
                        
        except Exception as e:
            logger.error(f"인덱스 자동 삭제 실패: {e}")
            
        return deleted_indices
    
    async def optimize_indices(self) -> bool:
        """인덱스 최적화"""
        try:
            # Force merge를 통한 세그먼트 최적화
            response = requests.post(f"{self.base_url}/_forcemerge?max_num_segments=1")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"인덱스 최적화 실패: {e}")
            return False


class PostgreSQLMonitor:
    """PostgreSQL 모니터링 및 관리"""
    
    def __init__(self, active_config: ServerConfig, standby_config: ServerConfig):
        self.active_config = active_config
        self.standby_config = standby_config
        
    async def check_connection(self, config: ServerConfig) -> bool:
        """DB 연결 상태 확인"""
        try:
            conn = psycopg2.connect(
                host=config.host,
                port=config.port,
                user=config.username,
                password=config.password,
                database='postgres',
                connect_timeout=5
            )
            conn.close()
            return True
        except Exception as e:
            logger.error(f"PostgreSQL 연결 실패 ({config.host}): {e}")
            return False
    
    async def check_replication_status(self) -> Dict[str, Any]:
        """복제 상태 확인"""
        try:
            conn = psycopg2.connect(
                host=self.active_config.host,
                port=self.active_config.port,
                user=self.active_config.username,
                password=self.active_config.password,
                database='postgres'
            )
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT client_addr, state, sync_state, 
                       pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) as lag_bytes
                FROM pg_stat_replication
            """)
            
            replication_info = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if replication_info:
                return {
                    'status': ComponentStatus.HEALTHY,
                    'standby_ip': replication_info[0][0],
                    'state': replication_info[0][1],
                    'sync_state': replication_info[0][2],
                    'lag_bytes': replication_info[0][3]
                }
            else:
                return {
                    'status': ComponentStatus.WARNING,
                    'message': 'No replication found'
                }
                
        except Exception as e:
            logger.error(f"복제 상태 확인 실패: {e}")
            return {'status': ComponentStatus.CRITICAL, 'error': str(e)}
    
    async def auto_vacuum_analyze(self) -> bool:
        """자동 VACUUM 및 ANALYZE 실행"""
        try:
            conn = psycopg2.connect(
                host=self.active_config.host,
                port=self.active_config.port,
                user=self.active_config.username,
                password=self.active_config.password,
                database='postgres'
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datname NOT IN ('template0', 'template1')")
            databases = cursor.fetchall()
            
            for db in databases:
                db_name = db[0]
                cursor.execute(f"VACUUM ANALYZE {db_name}")
                logger.info(f"VACUUM ANALYZE 실행 완료: {db_name}")
                
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"VACUUM ANALYZE 실패: {e}")
            return False
    
    async def check_slow_queries(self, threshold_ms: int = 5000) -> List[Dict[str, Any]]:
        """느린 쿼리 감지"""
        slow_queries = []
        try:
            conn = psycopg2.connect(
                host=self.active_config.host,
                port=self.active_config.port,
                user=self.active_config.username,
                password=self.active_config.password,
                database='postgres'
            )
            
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT pid, usename, query, 
                       extract(epoch from now() - query_start) * 1000 as duration_ms
                FROM pg_stat_activity
                WHERE state = 'active'
                  AND query NOT LIKE '%pg_stat_activity%'
                  AND extract(epoch from now() - query_start) * 1000 > {threshold_ms}
            """)
            
            for row in cursor.fetchall():
                slow_queries.append({
                    'pid': row[0],
                    'user': row[1],
                    'query': row[2][:100],  # 쿼리 일부만
                    'duration_ms': row[3]
                })
                
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"느린 쿼리 확인 실패: {e}")
            
        return slow_queries


class KafkaMonitor:
    """Kafka 모니터링 및 관리"""
    
    def __init__(self, bootstrap_servers: List[str]):
        self.bootstrap_servers = ','.join(bootstrap_servers)
        
    async def check_cluster_status(self) -> Dict[str, Any]:
        """Kafka 클러스터 상태 확인"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='pipeline_agent'
            )
            
            # 브로커 메타데이터 조회
            metadata = admin_client._client.cluster
            
            broker_count = len(metadata.brokers())
            topic_count = len(metadata.topics())
            
            status = ComponentStatus.HEALTHY if broker_count >= 2 else ComponentStatus.WARNING
            
            return {
                'status': status,
                'broker_count': broker_count,
                'topic_count': topic_count,
                'brokers': [f"{broker.host}:{broker.port}" for broker in metadata.brokers()]
            }
            
        except Exception as e:
            logger.error(f"Kafka 클러스터 상태 확인 실패: {e}")
            return {'status': ComponentStatus.UNKNOWN, 'error': str(e)}
    
    async def check_consumer_lag(self) -> List[Dict[str, Any]]:
        """컨슈머 랙 확인"""
        lag_info = []
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id='lag_checker',
                enable_auto_commit=False
            )
            
            # 각 토픽의 파티션별 랙 확인
            for topic in consumer.topics():
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    for partition in partitions:
                        # 현재 오프셋과 최신 오프셋 비교
                        committed = consumer.committed(partition)
                        if committed:
                            end_offset = consumer.end_offsets([partition])[partition]
                            lag = end_offset - committed
                            
                            if lag > 1000:  # 1000개 이상 밀림
                                lag_info.append({
                                    'topic': topic,
                                    'partition': partition,
                                    'lag': lag
                                })
                                
            consumer.close()
            
        except Exception as e:
            logger.error(f"컨슈머 랙 확인 실패: {e}")
            
        return lag_info
    
    async def auto_delete_old_topics(self, days: int = 7) -> List[str]:
        """오래된 토픽 자동 삭제"""
        deleted_topics = []
        # 구현은 토픽 네이밍 규칙에 따라 조정 필요
        return deleted_topics


class LogstashMonitor:
    """Logstash 모니터링 및 관리"""
    
    def __init__(self, filter_servers: List[ServerConfig], indexer_servers: List[ServerConfig]):
        self.filter_servers = filter_servers
        self.indexer_servers = indexer_servers
        
    async def check_process_status(self, server: ServerConfig) -> Dict[str, Any]:
        """Logstash 프로세스 상태 확인"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if server.ssh_key_path:
                ssh.connect(server.host, username=server.username, key_filename=server.ssh_key_path)
            else:
                ssh.connect(server.host, username=server.username, password=server.password)
            
            # 프로세스 확인
            stdin, stdout, stderr = ssh.exec_command("ps aux | grep logstash | grep -v grep")
            process_output = stdout.read().decode()
            
            # CPU 및 메모리 사용률 확인
            stdin, stdout, stderr = ssh.exec_command("top -bn1 | grep logstash")
            top_output = stdout.read().decode()
            
            ssh.close()
            
            if process_output:
                # top 출력에서 CPU 사용률 추출
                cpu_usage = 0.0
                if top_output:
                    parts = top_output.split()
                    if len(parts) > 8:
                        cpu_usage = float(parts[8])
                
                status = ComponentStatus.HEALTHY
                if cpu_usage > 70:
                    status = ComponentStatus.WARNING
                elif cpu_usage > 90:
                    status = ComponentStatus.CRITICAL
                    
                return {
                    'status': status,
                    'running': True,
                    'cpu_usage': cpu_usage
                }
            else:
                return {
                    'status': ComponentStatus.CRITICAL,
                    'running': False
                }
                
        except Exception as e:
            logger.error(f"Logstash 프로세스 확인 실패 ({server.host}): {e}")
            return {'status': ComponentStatus.UNKNOWN, 'error': str(e)}
    
    async def auto_clean_logs(self, server: ServerConfig) -> bool:
        """로그 파일 자동 정리"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if server.ssh_key_path:
                ssh.connect(server.host, username=server.username, key_filename=server.ssh_key_path)
            else:
                ssh.connect(server.host, username=server.username, password=server.password)
            
            # logstash_rx_message.log 내용 비우기
            commands = [
                "cat /dev/null > /home/logstash/logstash-8.1.3_metric/logstash_rx_message.log",
                "cat /dev/null > /home/ingest/logstash-8.1.3_metric/logstash_rx_message.log"
            ]
            
            for cmd in commands:
                stdin, stdout, stderr = ssh.exec_command(cmd)
                error = stderr.read().decode()
                if error:
                    logger.warning(f"로그 정리 경고: {error}")
                    
            ssh.close()
            return True
            
        except Exception as e:
            logger.error(f"로그 자동 정리 실패 ({server.host}): {e}")
            return False
    
    async def restart_if_needed(self, server: ServerConfig, cpu_threshold: float = 70.0) -> bool:
        """필요시 Logstash 재시작"""
        try:
            status = await self.check_process_status(server)
            
            if not status.get('running') or status.get('cpu_usage', 0) > cpu_threshold:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                if server.ssh_key_path:
                    ssh.connect(server.host, username=server.username, key_filename=server.ssh_key_path)
                else:
                    ssh.connect(server.host, username=server.username, password=server.password)
                
                # Logstash 재시작
                commands = [
                    "systemctl stop logstash",
                    "sleep 5",
                    "systemctl start logstash"
                ]
                
                for cmd in commands:
                    stdin, stdout, stderr = ssh.exec_command(cmd)
                    stdout.read()
                    
                ssh.close()
                logger.info(f"Logstash 재시작 완료: {server.host}")
                return True
                
        except Exception as e:
            logger.error(f"Logstash 재시작 실패 ({server.host}): {e}")
            
        return False


class DataPipelineAgent:
    """데이터 파이프라인 통합 관리 Agent"""
    
    def __init__(self, config_file: str):
        self.config = self.load_config(config_file)
        self.opensearch_monitor = None
        self.postgresql_monitor = None
        self.kafka_monitor = None
        self.logstash_monitor = None
        self.alert_config = None
        self.initialize_monitors()
        
    def load_config(self, config_file: str) -> Dict[str, Any]:
        """설정 파일 로드"""
        with open(config_file, 'r') as f:
            return json.load(f)
    
    def initialize_monitors(self):
        """모니터링 객체 초기화"""
        # OpenSearch
        if 'opensearch' in self.config:
            os_config = self.config['opensearch']
            self.opensearch_monitor = OpenSearchMonitor(
                master_host=os_config['master_host'],
                data_nodes=os_config['data_nodes']
            )
        
        # PostgreSQL
        if 'postgresql' in self.config:
            pg_config = self.config['postgresql']
            active_config = ServerConfig(**pg_config['active'])
            standby_config = ServerConfig(**pg_config['standby'])
            self.postgresql_monitor = PostgreSQLMonitor(active_config, standby_config)
        
        # Kafka
        if 'kafka' in self.config:
            self.kafka_monitor = KafkaMonitor(self.config['kafka']['bootstrap_servers'])
        
        # Logstash
        if 'logstash' in self.config:
            ls_config = self.config['logstash']
            filter_servers = [ServerConfig(**s) for s in ls_config['filter_servers']]
            indexer_servers = [ServerConfig(**s) for s in ls_config['indexer_servers']]
            self.logstash_monitor = LogstashMonitor(filter_servers, indexer_servers)
        
        # Alert 설정
        if 'alerts' in self.config:
            self.alert_config = AlertConfig(**self.config['alerts'])
    
    async def check_all_components(self) -> Dict[str, Any]:
        """모든 컴포넌트 상태 확인"""
        status_report = {
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        # OpenSearch 확인
        if self.opensearch_monitor:
            status_report['components']['opensearch'] = await self.opensearch_monitor.check_cluster_health()
        
        # PostgreSQL 확인
        if self.postgresql_monitor:
            active_status = await self.postgresql_monitor.check_connection(self.postgresql_monitor.active_config)
            standby_status = await self.postgresql_monitor.check_connection(self.postgresql_monitor.standby_config)
            replication = await self.postgresql_monitor.check_replication_status()
            
            status_report['components']['postgresql'] = {
                'active': active_status,
                'standby': standby_status,
                'replication': replication
            }
        
        # Kafka 확인
        if self.kafka_monitor:
            status_report['components']['kafka'] = await self.kafka_monitor.check_cluster_status()
        
        # Logstash 확인
        if self.logstash_monitor:
            filter_status = []
            indexer_status = []
            
            for server in self.logstash_monitor.filter_servers:
                filter_status.append(await self.logstash_monitor.check_process_status(server))
            
            for server in self.logstash_monitor.indexer_servers:
                indexer_status.append(await self.logstash_monitor.check_process_status(server))
            
            status_report['components']['logstash'] = {
                'filters': filter_status,
                'indexers': indexer_status
            }
        
        return status_report
    
    def send_alert(self, subject: str, message: str, severity: str = 'WARNING'):
        """알림 발송"""
        if not self.alert_config:
            return
        
        # 이메일 알림
        if self.alert_config.email_enabled and self.alert_config.email_recipients:
            try:
                msg = MIMEMultipart()
                msg['Subject'] = f"[{severity}] Data Pipeline Alert: {subject}"
                msg['From'] = "pipeline-agent@company.com"
                msg['To'] = ', '.join(self.alert_config.email_recipients)
                
                body = f"""
                Alert Time: {datetime.now().isoformat()}
                Severity: {severity}
                Subject: {subject}
                
                Details:
                {message}
                """
                
                msg.attach(MIMEText(body, 'plain'))
                
                # SMTP 서버 설정 필요
                # smtp = smtplib.SMTP('smtp.company.com', 587)
                # smtp.send_message(msg)
                # smtp.quit()
                
                logger.info(f"알림 발송: {subject}")
                
            except Exception as e:
                logger.error(f"이메일 알림 발송 실패: {e}")
        
        # Slack 알림
        if self.alert_config.slack_enabled and self.alert_config.slack_webhook:
            try:
                slack_data = {
                    'text': f"*[{severity}]* {subject}",
                    'attachments': [{
                        'color': 'danger' if severity == 'CRITICAL' else 'warning',
                        'text': message,
                        'ts': int(time.time())
                    }]
                }
                
                response = requests.post(
                    self.alert_config.slack_webhook,
                    json=slack_data
                )
                
                if response.status_code != 200:
                    logger.error(f"Slack 알림 실패: {response.text}")
                    
            except Exception as e:
                logger.error(f"Slack 알림 발송 실패: {e}")
    
    async def auto_maintenance(self):
        """자동 유지보수 작업"""
        logger.info("자동 유지보수 작업 시작")
        
        # OpenSearch 오래된 인덱스 삭제
        if self.opensearch_monitor:
            deleted = await self.opensearch_monitor.auto_delete_old_indices(days=30)
            if deleted:
                logger.info(f"삭제된 인덱스: {deleted}")
        
        # PostgreSQL VACUUM
        if self.postgresql_monitor:
            await self.postgresql_monitor.auto_vacuum_analyze()
        
        # Logstash 로그 정리
        if self.logstash_monitor:
            for server in self.logstash_monitor.filter_servers:
                await self.logstash_monitor.auto_clean_logs(server)
            for server in self.logstash_monitor.indexer_servers:
                await self.logstash_monitor.auto_clean_logs(server)
        
        logger.info("자동 유지보수 작업 완료")
    
    async def monitor_loop(self):
        """모니터링 루프"""
        while True:
            try:
                # 상태 확인
                status_report = await self.check_all_components()
                
                # 문제 감지 및 알림
                for component, status in status_report['components'].items():
                    if isinstance(status, dict) and status.get('status') == ComponentStatus.CRITICAL:
                        self.send_alert(
                            f"{component} Critical Issue",
                            json.dumps(status, indent=2),
                            'CRITICAL'
                        )
                
                # 30초 대기
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"모니터링 루프 오류: {e}")
                await asyncio.sleep(60)
    
    def run(self):
        """Agent 실행"""
        logger.info("Data Pipeline Agent 시작")
        
        # 스케줄 설정 (매일 자정 유지보수)
        schedule.every().day.at("00:00").do(lambda: asyncio.run(self.auto_maintenance()))
        
        # 비동기 루프 실행
        loop = asyncio.get_event_loop()
        
        # 초기 상태 확인
        loop.run_until_complete(self.check_all_components())
        
        # 모니터링 루프 실행
        try:
            loop.run_until_complete(self.monitor_loop())
        except KeyboardInterrupt:
            logger.info("Agent 종료")
        finally:
            loop.close()


if __name__ == "__main__":
    # 설정 파일 경로
    config_file = "/etc/pipeline_agent/config.json"
    
    # Agent 실행
    agent = DataPipelineAgent(config_file)
    agent.run()