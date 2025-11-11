#!/usr/bin/env python3

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
import warnings
warnings.filterwarnings('ignore')


class AnomalyDetector:
    """이상 탐지 AI 모델"""
    
    def __init__(self):
        self.model = IsolationForest(
            contamination=0.1,
            random_state=42,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        
    def prepare_features(self, metrics: Dict[str, Any]) -> np.ndarray:
        """메트릭을 특징 벡터로 변환"""
        features = []
        
        # CPU 사용률
        features.append(metrics.get('cpu_usage', 0))
        
        # 메모리 사용률
        features.append(metrics.get('memory_usage', 0))
        
        # 디스크 사용률
        features.append(metrics.get('disk_usage', 0))
        
        # 네트워크 트래픽
        features.append(metrics.get('network_in', 0))
        features.append(metrics.get('network_out', 0))
        
        # 쿼리 응답 시간
        features.append(metrics.get('query_latency', 0))
        
        # 에러율
        features.append(metrics.get('error_rate', 0))
        
        # 처리량
        features.append(metrics.get('throughput', 0))
        
        return np.array(features).reshape(1, -1)
    
    def train(self, historical_data: pd.DataFrame):
        """과거 데이터로 모델 학습"""
        if historical_data.empty:
            return False
        
        # 특징 추출
        features = []
        for _, row in historical_data.iterrows():
            features.append(self.prepare_features(row.to_dict()).flatten())
        
        X = np.array(features)
        
        # 정규화
        X_scaled = self.scaler.fit_transform(X)
        
        # 모델 학습
        self.model.fit(X_scaled)
        self.is_trained = True
        
        return True
    
    def detect(self, current_metrics: Dict[str, Any]) -> Tuple[bool, float]:
        """이상 탐지 수행"""
        if not self.is_trained:
            return False, 0.0
        
        # 특징 추출 및 정규화
        features = self.prepare_features(current_metrics)
        features_scaled = self.scaler.transform(features)
        
        # 예측 (1: 정상, -1: 이상)
        prediction = self.model.predict(features_scaled)[0]
        
        # 이상 점수 계산 (낮을수록 이상)
        anomaly_score = self.model.score_samples(features_scaled)[0]
        
        is_anomaly = prediction == -1
        confidence = abs(anomaly_score)
        
        return is_anomaly, confidence
    
    def save_model(self, path: str):
        """모델 저장"""
        joblib.dump({
            'model': self.model,
            'scaler': self.scaler,
            'is_trained': self.is_trained
        }, path)
    
    def load_model(self, path: str):
        """모델 로드"""
        data = joblib.load(path)
        self.model = data['model']
        self.scaler = data['scaler']
        self.is_trained = data['is_trained']


class PerformancePredictor:
    """성능 예측 AI 모델"""
    
    def __init__(self):
        self.cpu_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        self.memory_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        self.disk_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        self.is_trained = False
        
    def prepare_time_features(self, timestamp: datetime) -> List[float]:
        """시간 기반 특징 추출"""
        features = [
            timestamp.hour,
            timestamp.dayofweek,
            timestamp.day,
            timestamp.month,
            timestamp.minute,
            int(timestamp.timestamp())
        ]
        return features
    
    def train(self, historical_data: pd.DataFrame):
        """과거 데이터로 예측 모델 학습"""
        if len(historical_data) < 100:
            return False
        
        # 시간 특징 추출
        X = []
        y_cpu = []
        y_memory = []
        y_disk = []
        
        for _, row in historical_data.iterrows():
            timestamp = pd.to_datetime(row['timestamp'])
            time_features = self.prepare_time_features(timestamp)
            
            X.append(time_features)
            y_cpu.append(row['cpu_usage'])
            y_memory.append(row['memory_usage'])
            y_disk.append(row['disk_usage'])
        
        X = np.array(X)
        
        # 각 메트릭별 모델 학습
        self.cpu_predictor.fit(X, y_cpu)
        self.memory_predictor.fit(X, y_memory)
        self.disk_predictor.fit(X, y_disk)
        
        self.is_trained = True
        return True
    
    def predict(self, future_timestamp: datetime) -> Dict[str, float]:
        """미래 시점의 성능 예측"""
        if not self.is_trained:
            return {}
        
        # 시간 특징 추출
        time_features = np.array([self.prepare_time_features(future_timestamp)])
        
        # 예측
        predictions = {
            'cpu_usage': float(self.cpu_predictor.predict(time_features)[0]),
            'memory_usage': float(self.memory_predictor.predict(time_features)[0]),
            'disk_usage': float(self.disk_predictor.predict(time_features)[0])
        }
        
        return predictions
    
    def predict_capacity_exhaustion(self, current_usage: float, growth_rate: float, 
                                   threshold: float = 90.0) -> Optional[datetime]:
        """용량 소진 시점 예측"""
        if current_usage >= threshold:
            return datetime.now()
        
        if growth_rate <= 0:
            return None
        
        # 선형 예측
        days_until_exhaustion = (threshold - current_usage) / growth_rate
        
        if days_until_exhaustion > 365:  # 1년 이상이면 None
            return None
        
        return datetime.now() + timedelta(days=int(days_until_exhaustion))


class IntelligentOptimizer:
    """AI 기반 최적화 제안 시스템"""
    
    def __init__(self):
        self.optimization_rules = self.load_optimization_rules()
        
    def load_optimization_rules(self) -> Dict[str, List[Dict]]:
        """최적화 규칙 로드"""
        return {
            'opensearch': [
                {
                    'condition': lambda m: m.get('heap_usage', 0) > 80,
                    'action': 'increase_heap_size',
                    'params': {'increase_by': '2G'}
                },
                {
                    'condition': lambda m: m.get('query_latency', 0) > 1000,
                    'action': 'optimize_indices',
                    'params': {'max_num_segments': 1}
                },
                {
                    'condition': lambda m: m.get('unassigned_shards', 0) > 0,
                    'action': 'reallocate_shards',
                    'params': {}
                }
            ],
            'postgresql': [
                {
                    'condition': lambda m: m.get('cache_hit_ratio', 100) < 90,
                    'action': 'increase_shared_buffers',
                    'params': {'increase_by': '256MB'}
                },
                {
                    'condition': lambda m: m.get('slow_query_count', 0) > 10,
                    'action': 'analyze_slow_queries',
                    'params': {'create_indexes': True}
                },
                {
                    'condition': lambda m: m.get('replication_lag', 0) > 1000000,
                    'action': 'optimize_replication',
                    'params': {'wal_keep_segments': 100}
                }
            ],
            'kafka': [
                {
                    'condition': lambda m: m.get('consumer_lag', 0) > 10000,
                    'action': 'scale_consumers',
                    'params': {'increase_by': 2}
                },
                {
                    'condition': lambda m: m.get('disk_usage', 0) > 80,
                    'action': 'adjust_retention',
                    'params': {'retention_hours': 24}
                }
            ],
            'logstash': [
                {
                    'condition': lambda m: m.get('cpu_usage', 0) > 80,
                    'action': 'increase_workers',
                    'params': {'workers': 8}
                },
                {
                    'condition': lambda m: m.get('processing_rate', 0) < 1000,
                    'action': 'optimize_pipeline',
                    'params': {'batch_size': 250}
                }
            ]
        }
    
    def analyze(self, component: str, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """컴포넌트 분석 및 최적화 제안"""
        recommendations = []
        
        if component not in self.optimization_rules:
            return recommendations
        
        for rule in self.optimization_rules[component]:
            if rule['condition'](metrics):
                recommendations.append({
                    'component': component,
                    'action': rule['action'],
                    'params': rule['params'],
                    'reason': f"Metric exceeded threshold",
                    'priority': self.calculate_priority(metrics)
                })
        
        return recommendations
    
    def calculate_priority(self, metrics: Dict[str, Any]) -> str:
        """우선순위 계산"""
        critical_metrics = ['cpu_usage', 'memory_usage', 'disk_usage']
        
        max_usage = max([metrics.get(m, 0) for m in critical_metrics])
        
        if max_usage > 90:
            return 'CRITICAL'
        elif max_usage > 75:
            return 'HIGH'
        elif max_usage > 60:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def generate_optimization_script(self, recommendation: Dict[str, Any]) -> str:
        """최적화 실행 스크립트 생성"""
        component = recommendation['component']
        action = recommendation['action']
        params = recommendation['params']
        
        script_templates = {
            'opensearch': {
                'increase_heap_size': f"""
                    # OpenSearch Heap Size 증가
                    sed -i 's/-Xmx[0-9]*g/-Xmx{params.get('increase_by', '4G')}/g' /etc/opensearch/jvm.options
                    systemctl restart opensearch
                """,
                'optimize_indices': f"""
                    # 인덱스 최적화
                    curl -X POST "localhost:9200/_forcemerge?max_num_segments={params.get('max_num_segments', 1)}"
                """,
                'reallocate_shards': """
                    # 샤드 재할당
                    curl -X POST "localhost:9200/_cluster/reroute?retry_failed=true"
                """
            },
            'postgresql': {
                'increase_shared_buffers': f"""
                    # PostgreSQL shared_buffers 증가
                    echo "shared_buffers = {params.get('increase_by', '256MB')}" >> /etc/postgresql/postgresql.conf
                    systemctl restart postgresql
                """,
                'analyze_slow_queries': """
                    # 느린 쿼리 분석
                    psql -c "SELECT pg_stat_statements_reset();"
                    psql -c "ANALYZE;"
                """
            },
            'kafka': {
                'scale_consumers': f"""
                    # Kafka Consumer 스케일링
                    # Consumer Group의 인스턴스를 {params.get('increase_by', 2)}개 추가
                    for i in {{1..{params.get('increase_by', 2)}}}; do
                        nohup java -jar kafka-consumer.jar &
                    done
                """,
                'adjust_retention': f"""
                    # Kafka Retention 조정
                    kafka-configs.sh --alter --topic all --config retention.hours={params.get('retention_hours', 24)}
                """
            },
            'logstash': {
                'increase_workers': f"""
                    # Logstash Worker 증가
                    sed -i 's/pipeline.workers: [0-9]*/pipeline.workers: {params.get('workers', 8)}/g' /etc/logstash/logstash.yml
                    systemctl restart logstash
                """,
                'optimize_pipeline': f"""
                    # Logstash Pipeline 최적화
                    sed -i 's/pipeline.batch.size: [0-9]*/pipeline.batch.size: {params.get('batch_size', 250)}/g' /etc/logstash/logstash.yml
                    systemctl restart logstash
                """
            }
        }
        
        if component in script_templates and action in script_templates[component]:
            return script_templates[component][action]
        
        return "# No script template available"


class PredictiveMaintenanceSystem:
    """예측 유지보수 시스템"""
    
    def __init__(self):
        self.anomaly_detector = AnomalyDetector()
        self.performance_predictor = PerformancePredictor()
        self.optimizer = IntelligentOptimizer()
        self.incident_history = []
        
    def analyze_component_health(self, component: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """컴포넌트 건강상태 종합 분석"""
        analysis = {
            'component': component,
            'timestamp': datetime.now().isoformat(),
            'current_status': 'HEALTHY',
            'risk_level': 'LOW',
            'predictions': {},
            'recommendations': []
        }
        
        # 이상 탐지
        if self.anomaly_detector.is_trained:
            is_anomaly, confidence = self.anomaly_detector.detect(metrics)
            if is_anomaly:
                analysis['current_status'] = 'ANOMALY_DETECTED'
                analysis['anomaly_confidence'] = confidence
                analysis['risk_level'] = 'HIGH' if confidence > 0.8 else 'MEDIUM'
        
        # 성능 예측
        if self.performance_predictor.is_trained:
            # 1시간 후 예측
            future_1h = datetime.now() + timedelta(hours=1)
            predictions_1h = self.performance_predictor.predict(future_1h)
            
            # 24시간 후 예측
            future_24h = datetime.now() + timedelta(hours=24)
            predictions_24h = self.performance_predictor.predict(future_24h)
            
            analysis['predictions'] = {
                '1_hour': predictions_1h,
                '24_hours': predictions_24h
            }
            
            # 용량 소진 예측
            if 'disk_usage' in metrics:
                exhaustion_date = self.performance_predictor.predict_capacity_exhaustion(
                    metrics['disk_usage'], 
                    growth_rate=0.5  # 일일 0.5% 성장 가정
                )
                if exhaustion_date:
                    analysis['disk_exhaustion_date'] = exhaustion_date.isoformat()
        
        # 최적화 제안
        recommendations = self.optimizer.analyze(component, metrics)
        analysis['recommendations'] = recommendations
        
        # 리스크 레벨 재계산
        if recommendations:
            priorities = [r['priority'] for r in recommendations]
            if 'CRITICAL' in priorities:
                analysis['risk_level'] = 'CRITICAL'
            elif 'HIGH' in priorities:
                analysis['risk_level'] = 'HIGH'
        
        return analysis
    
    def predict_failure_probability(self, component_history: pd.DataFrame) -> float:
        """장애 발생 확률 예측"""
        if len(component_history) < 10:
            return 0.0
        
        # 최근 이상 패턴 분석
        recent_anomalies = 0
        for _, row in component_history.tail(10).iterrows():
            metrics = row.to_dict()
            if self.anomaly_detector.is_trained:
                is_anomaly, _ = self.anomaly_detector.detect(metrics)
                if is_anomaly:
                    recent_anomalies += 1
        
        # 확률 계산 (최근 10개 중 이상 비율)
        failure_probability = (recent_anomalies / 10) * 100
        
        return failure_probability
    
    def generate_maintenance_schedule(self, components: List[str]) -> List[Dict[str, Any]]:
        """유지보수 일정 자동 생성"""
        schedule = []
        
        for component in components:
            # 컴포넌트별 최적 유지보수 시간 계산
            maintenance_item = {
                'component': component,
                'scheduled_date': None,
                'estimated_duration': '30 minutes',
                'type': 'PREVENTIVE',
                'tasks': []
            }
            
            # 컴포넌트별 유지보수 작업
            if component == 'opensearch':
                maintenance_item['tasks'] = [
                    'Index optimization',
                    'Shard rebalancing',
                    'Cache clearing'
                ]
                maintenance_item['scheduled_date'] = (datetime.now() + timedelta(days=7)).isoformat()
                
            elif component == 'postgresql':
                maintenance_item['tasks'] = [
                    'VACUUM ANALYZE',
                    'Index rebuild',
                    'Statistics update'
                ]
                maintenance_item['scheduled_date'] = (datetime.now() + timedelta(days=3)).isoformat()
                
            elif component == 'kafka':
                maintenance_item['tasks'] = [
                    'Log compaction',
                    'Partition rebalancing',
                    'Consumer group cleanup'
                ]
                maintenance_item['scheduled_date'] = (datetime.now() + timedelta(days=5)).isoformat()
                
            elif component == 'logstash':
                maintenance_item['tasks'] = [
                    'Pipeline optimization',
                    'Log rotation',
                    'Memory cleanup'
                ]
                maintenance_item['scheduled_date'] = (datetime.now() + timedelta(days=1)).isoformat()
            
            schedule.append(maintenance_item)
        
        return sorted(schedule, key=lambda x: x['scheduled_date'])
    
    def learn_from_incident(self, incident: Dict[str, Any]):
        """인시던트로부터 학습"""
        self.incident_history.append({
            'timestamp': datetime.now().isoformat(),
            'incident': incident
        })
        
        # 패턴 분석 및 모델 재학습 트리거
        if len(self.incident_history) % 10 == 0:
            # 10개의 인시던트마다 모델 재학습
            logger.info("인시던트 패턴 학습 시작")
            # 실제 구현시 과거 데이터를 다시 로드하여 모델 재학습