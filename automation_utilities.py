#!/usr/bin/env python3

import os
import sys
import json
import yaml
import argparse
import subprocess
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AutomationScripts:
    """자동화 스크립트 모음"""
    
    @staticmethod
    def auto_restart_service(service_name: str, host: str = 'localhost') -> bool:
        """서비스 자동 재시작"""
        try:
            if host == 'localhost':
                cmd = f"sudo systemctl restart {service_name}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            else:
                cmd = f"ssh {host} 'sudo systemctl restart {service_name}'"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"서비스 재시작 성공: {service_name} on {host}")
                return True
            else:
                logger.error(f"서비스 재시작 실패: {service_name} on {host}")
                return False
                
        except Exception as e:
            logger.error(f"서비스 재시작 오류: {e}")
            return False
    
    @staticmethod
    def cleanup_logs(log_paths: List[str], max_age_days: int = 7) -> int:
        """오래된 로그 파일 정리"""
        deleted_count = 0
        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        
        for log_path in log_paths:
            try:
                if os.path.exists(log_path):
                    # 파일 수정 시간 확인
                    file_time = datetime.fromtimestamp(os.path.getmtime(log_path))
                    if file_time < cutoff_date:
                        os.remove(log_path)
                        deleted_count += 1
                        logger.info(f"로그 파일 삭제: {log_path}")
            except Exception as e:
                logger.error(f"로그 파일 삭제 실패 {log_path}: {e}")
        
        return deleted_count
    
    @staticmethod
    def backup_configuration(component: str, backup_dir: str = '/backup') -> bool:
        """설정 파일 백업"""
        config_paths = {
            'opensearch': '/etc/opensearch/',
            'postgresql': '/etc/postgresql/',
            'kafka': '/etc/kafka/',
            'logstash': '/etc/logstash/'
        }
        
        if component not in config_paths:
            logger.error(f"알 수 없는 컴포넌트: {component}")
            return False
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"{backup_dir}/{component}_{timestamp}.tar.gz"
            
            cmd = f"tar -czf {backup_path} {config_paths[component]}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"백업 완료: {backup_path}")
                return True
            else:
                logger.error(f"백업 실패: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"백업 오류: {e}")
            return False
    
    @staticmethod
    def scale_service(service: str, action: str = 'up', count: int = 1) -> bool:
        """서비스 스케일링"""
        try:
            if service == 'logstash':
                if action == 'up':
                    # Logstash 인스턴스 추가
                    for i in range(count):
                        port = 5044 + i + 1
                        cmd = f"""
                        cp -r /etc/logstash /etc/logstash_{port}
                        sed -i 's/5044/{port}/g' /etc/logstash_{port}/pipelines.yml
                        systemctl start logstash@{port}
                        """
                        subprocess.run(cmd, shell=True)
                        logger.info(f"Logstash 인스턴스 추가: port {port}")
                elif action == 'down':
                    # Logstash 인스턴스 제거
                    for i in range(count):
                        port = 5044 + i + 1
                        cmd = f"systemctl stop logstash@{port}"
                        subprocess.run(cmd, shell=True)
                        logger.info(f"Logstash 인스턴스 제거: port {port}")
            
            return True
            
        except Exception as e:
            logger.error(f"스케일링 오류: {e}")
            return False


class HealthChecker:
    """헬스 체크 유틸리티"""
    
    @staticmethod
    def check_opensearch_health(host: str = 'localhost', port: int = 9200) -> Dict:
        """OpenSearch 헬스 체크"""
        try:
            response = requests.get(f"http://{host}:{port}/_cluster/health", timeout=5)
            if response.status_code == 200:
                health = response.json()
                return {
                    'status': 'healthy' if health['status'] == 'green' else 'unhealthy',
                    'details': health
                }
        except:
            return {'status': 'down', 'details': None}
    
    @staticmethod
    def check_postgresql_health(host: str, port: int, user: str, password: str) -> Dict:
        """PostgreSQL 헬스 체크"""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password,
                database='postgres', connect_timeout=5
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return {'status': 'healthy', 'details': {'connection': 'ok'}}
        except Exception as e:
            return {'status': 'down', 'details': {'error': str(e)}}
    
    @staticmethod
    def check_kafka_health(bootstrap_servers: str) -> Dict:
        """Kafka 헬스 체크"""
        try:
            from kafka import KafkaAdminClient
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='health_checker',
                request_timeout_ms=5000
            )
            metadata = admin_client._client.cluster
            broker_count = len(metadata.brokers())
            
            return {
                'status': 'healthy' if broker_count > 0 else 'unhealthy',
                'details': {'broker_count': broker_count}
            }
        except Exception as e:
            return {'status': 'down', 'details': {'error': str(e)}}
    
    @staticmethod
    def check_logstash_health(host: str = 'localhost', port: int = 9600) -> Dict:
        """Logstash 헬스 체크"""
        try:
            response = requests.get(f"http://{host}:{port}/_node/stats", timeout=5)
            if response.status_code == 200:
                stats = response.json()
                return {
                    'status': 'healthy',
                    'details': {
                        'pipeline_count': len(stats.get('pipelines', {})),
                        'cpu_usage': stats.get('process', {}).get('cpu', {}).get('percent', 0)
                    }
                }
        except:
            return {'status': 'down', 'details': None}


class DisasterRecovery:
    """재해 복구 유틸리티"""
    
    @staticmethod
    def backup_all_components(backup_dir: str = '/backup/dr') -> bool:
        """모든 컴포넌트 백업"""
        components = ['opensearch', 'postgresql', 'kafka', 'logstash']
        success = True
        
        for component in components:
            if not AutomationScripts.backup_configuration(component, backup_dir):
                success = False
                logger.error(f"{component} 백업 실패")
        
        return success
    
    @staticmethod
    def failover_postgresql(standby_host: str) -> bool:
        """PostgreSQL 페일오버"""
        try:
            # Standby를 Primary로 승격
            cmd = f"ssh {standby_host} 'sudo -u postgres pg_ctl promote -D /var/lib/postgresql/data'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"PostgreSQL 페일오버 성공: {standby_host}가 Primary로 승격")
                return True
            else:
                logger.error(f"PostgreSQL 페일오버 실패: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"페일오버 오류: {e}")
            return False
    
    @staticmethod
    def restore_from_backup(component: str, backup_file: str) -> bool:
        """백업에서 복원"""
        config_paths = {
            'opensearch': '/etc/opensearch/',
            'postgresql': '/etc/postgresql/',
            'kafka': '/etc/kafka/',
            'logstash': '/etc/logstash/'
        }
        
        if component not in config_paths:
            return False
        
        try:
            # 기존 설정 백업
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            cmd = f"mv {config_paths[component]} {config_paths[component]}.bak_{timestamp}"
            subprocess.run(cmd, shell=True)
            
            # 백업 파일 복원
            cmd = f"tar -xzf {backup_file} -C /"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"복원 완료: {component} from {backup_file}")
                
                # 서비스 재시작
                AutomationScripts.auto_restart_service(component)
                return True
            else:
                logger.error(f"복원 실패: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"복원 오류: {e}")
            return False


class PerformanceTuner:
    """성능 튜닝 유틸리티"""
    
    @staticmethod
    def tune_opensearch(heap_size: str = '4g', indices_memory: str = '50%'):
        """OpenSearch 성능 튜닝"""
        try:
            # JVM 힙 크기 조정
            jvm_options = f"""
-Xms{heap_size}
-Xmx{heap_size}
"""
            with open('/etc/opensearch/jvm.options', 'w') as f:
                f.write(jvm_options)
            
            # 인덱스 메모리 설정
            settings = {
                "index": {
                    "refresh_interval": "30s",
                    "number_of_replicas": 1,
                    "translog": {
                        "durability": "async",
                        "sync_interval": "30s"
                    }
                }
            }
            
            response = requests.put(
                "http://localhost:9200/_settings",
                json=settings
            )
            
            logger.info("OpenSearch 튜닝 완료")
            return True
            
        except Exception as e:
            logger.error(f"OpenSearch 튜닝 실패: {e}")
            return False
    
    @staticmethod
    def tune_postgresql(shared_buffers: str = '256MB', work_mem: str = '4MB'):
        """PostgreSQL 성능 튜닝"""
        try:
            config_updates = f"""
# Performance Tuning
shared_buffers = {shared_buffers}
work_mem = {work_mem}
maintenance_work_mem = 256MB
effective_cache_size = 4GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
random_page_cost = 1.1
effective_io_concurrency = 200
"""
            
            with open('/etc/postgresql/postgresql.conf', 'a') as f:
                f.write(config_updates)
            
            # PostgreSQL 재시작
            AutomationScripts.auto_restart_service('postgresql')
            
            logger.info("PostgreSQL 튜닝 완료")
            return True
            
        except Exception as e:
            logger.error(f"PostgreSQL 튜닝 실패: {e}")
            return False
    
    @staticmethod
    def tune_kafka(heap_size: str = '6g', num_network_threads: int = 8):
        """Kafka 성능 튜닝"""
        try:
            # JVM 설정
            kafka_env = f"""
export KAFKA_HEAP_OPTS="-Xmx{heap_size} -Xms{heap_size}"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=20"
"""
            with open('/etc/kafka/kafka-env.sh', 'w') as f:
                f.write(kafka_env)
            
            # Server properties 튜닝
            server_properties = f"""
num.network.threads={num_network_threads}
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=3
num.recovery.threads.per.data.dir=1
log.retention.hours=168
log.segment.bytes=1073741824
"""
            
            with open('/etc/kafka/server.properties', 'a') as f:
                f.write(server_properties)
            
            logger.info("Kafka 튜닝 완료")
            return True
            
        except Exception as e:
            logger.error(f"Kafka 튜닝 실패: {e}")
            return False
    
    @staticmethod
    def tune_logstash(workers: int = 4, batch_size: int = 125):
        """Logstash 성능 튜닝"""
        try:
            logstash_yml = f"""
pipeline.workers: {workers}
pipeline.batch.size: {batch_size}
pipeline.batch.delay: 50
config.reload.automatic: true
config.reload.interval: 3s
"""
            
            with open('/etc/logstash/logstash.yml', 'w') as f:
                f.write(logstash_yml)
            
            # JVM 옵션
            jvm_options = """
-Xms2g
-Xmx2g
-XX:+UseG1GC
"""
            
            with open('/etc/logstash/jvm.options', 'w') as f:
                f.write(jvm_options)
            
            logger.info("Logstash 튜닝 완료")
            return True
            
        except Exception as e:
            logger.error(f"Logstash 튜닝 실패: {e}")
            return False


def main():
    """메인 CLI 인터페이스"""
    parser = argparse.ArgumentParser(description='데이터 파이프라인 자동화 유틸리티')
    
    subparsers = parser.add_subparsers(dest='command', help='명령어')
    
    # 헬스 체크
    health_parser = subparsers.add_parser('health', help='컴포넌트 헬스 체크')
    health_parser.add_argument('component', choices=['all', 'opensearch', 'postgresql', 'kafka', 'logstash'])
    
    # 백업
    backup_parser = subparsers.add_parser('backup', help='컴포넌트 백업')
    backup_parser.add_argument('component', choices=['all', 'opensearch', 'postgresql', 'kafka', 'logstash'])
    backup_parser.add_argument('--dir', default='/backup', help='백업 디렉토리')
    
    # 복원
    restore_parser = subparsers.add_parser('restore', help='백업에서 복원')
    restore_parser.add_argument('component', choices=['opensearch', 'postgresql', 'kafka', 'logstash'])
    restore_parser.add_argument('backup_file', help='백업 파일 경로')
    
    # 튜닝
    tune_parser = subparsers.add_parser('tune', help='성능 튜닝')
    tune_parser.add_argument('component', choices=['all', 'opensearch', 'postgresql', 'kafka', 'logstash'])
    
    # 스케일
    scale_parser = subparsers.add_parser('scale', help='서비스 스케일링')
    scale_parser.add_argument('service', choices=['logstash'])
    scale_parser.add_argument('action', choices=['up', 'down'])
    scale_parser.add_argument('--count', type=int, default=1, help='스케일 수')
    
    # 페일오버
    failover_parser = subparsers.add_parser('failover', help='페일오버 실행')
    failover_parser.add_argument('component', choices=['postgresql'])
    failover_parser.add_argument('standby_host', help='Standby 서버 호스트')
    
    args = parser.parse_args()
    
    if args.command == 'health':
        if args.component == 'all':
            components = ['opensearch', 'postgresql', 'kafka', 'logstash']
        else:
            components = [args.component]
        
        for comp in components:
            if comp == 'opensearch':
                status = HealthChecker.check_opensearch_health()
            elif comp == 'postgresql':
                status = HealthChecker.check_postgresql_health('localhost', 5432, 'postgres', 'password')
            elif comp == 'kafka':
                status = HealthChecker.check_kafka_health('localhost:9092')
            elif comp == 'logstash':
                status = HealthChecker.check_logstash_health()
            
            print(f"{comp}: {json.dumps(status, indent=2)}")
    
    elif args.command == 'backup':
        if args.component == 'all':
            DisasterRecovery.backup_all_components(args.dir)
        else:
            AutomationScripts.backup_configuration(args.component, args.dir)
    
    elif args.command == 'restore':
        DisasterRecovery.restore_from_backup(args.component, args.backup_file)
    
    elif args.command == 'tune':
        if args.component == 'all':
            PerformanceTuner.tune_opensearch()
            PerformanceTuner.tune_postgresql()
            PerformanceTuner.tune_kafka()
            PerformanceTuner.tune_logstash()
        elif args.component == 'opensearch':
            PerformanceTuner.tune_opensearch()
        elif args.component == 'postgresql':
            PerformanceTuner.tune_postgresql()
        elif args.component == 'kafka':
            PerformanceTuner.tune_kafka()
        elif args.component == 'logstash':
            PerformanceTuner.tune_logstash()
    
    elif args.command == 'scale':
        AutomationScripts.scale_service(args.service, args.action, args.count)
    
    elif args.command == 'failover':
        if args.component == 'postgresql':
            DisasterRecovery.failover_postgresql(args.standby_host)


if __name__ == "__main__":
    main()