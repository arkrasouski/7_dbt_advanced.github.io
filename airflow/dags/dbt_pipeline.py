from airflow.decorators import dag, task

from datetime import datetime, timedelta
import logging
from typing import Dict, Any

default_args = {
    'owner': 'arkrasouski',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['arkrasouski@arortem.ru'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_display_name='Мониторинг и алертинг',
    description='Даг, реализующий сбор и рассылку статистики dbt run',
    default_args = default_args,
    schedule=None,
    start_date=datetime(2026, 7, 3),
    catchup=False,
    tags=['']

)


def dbt_pipeline():
    def dbt_execute(command_type: str):
        dbt_profile = '--profiles-dir /opt/airflow/.dbt'
        return f"""
                cd /opt/dbt/hw1
                dbt seed {dbt_profile}
                dbt {command_type} {dbt_profile}
               """

    @task.bash
    def dbt_test():
        return dbt_execute('test')

    @task.bash
    def dbt_run():
        return dbt_execute('run')

    @task
    def parse_dbt_results(artifact_path: str) -> Dict[str, Any]:
        """Парсинг результатов dbt run"""
        import json
        import os
        
        results_file = os.path.join(artifact_path, 'target', 'run_results.json')
        manifest_file = os.path.join(artifact_path, 'target', 'manifest.json')
        
        stats = {
            'total_models': 0,
            'successful_models': 0,
            'failed_models': 0,
            'skipped_models': 0,
            'error_models': 0,
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'execution_time': 0,
            'models_performance': [],
            'rows_processed': {}
        }
        
        # Парсинг run_results
        if os.path.exists(results_file):
            with open(results_file, 'r') as f:###
                data = json.load(f)
                for result in data.get('results', []):
                    # Статистика по моделям
                    if result.get('unique_id', '').startswith('model'):
                        stats['total_models'] += 1
                        status = result.get('status')
                        if status == 'success':
                            stats['successful_models'] += 1
                        elif status == 'error':
                            stats['failed_models'] += 1
                        elif status == 'skipped':
                            stats['skipped_models'] += 1
                        
                        # Время выполнения
                        execution_time = result.get('execution_time', 0)
                        stats['execution_time'] += execution_time
                        
                        # Сбор информации о производительности
                        model_name = result.get('unique_id', '').split('.')[-1]
                        stats['models_performance'].append({
                            'model': model_name,
                            'execution_time': execution_time,
                            'rows_affected': result.get('adapter_response', {}).get('rows_affected', 0),
                            'status': status
                        })
                    
                    # Статистика по тестам
                    elif result.get('unique_id', '').startswith('test'):
                        stats['total_tests'] += 1
                        if result.get('status') == 'pass':
                            stats['passed_tests'] += 1
                        else:
                            stats['failed_tests'] += 1
        
        # Парсинг manifest для получения количества строк
        if os.path.exists(manifest_file):
            with open(manifest_file, 'r') as f:
                manifest = json.load(f)
                for node_id, node in manifest.get('nodes', {}).items():
                    if node.get('resource_type') == 'model':
                        # Можно добавить информацию о количестве строк из метаданных
                        stats['rows_processed'][node.get('name')] = node.get('config', {}).get('meta', {}).get('row_count', 0)
        
        return stats
    
    dbt_test_task = dbt_test()

    dbt_run_task = dbt_run()

    stats_task = parse_dbt_results('/opt/dbt/hw1')
    

    dbt_test_task >> dbt_run_task >> stats_task


dbt_pipeline()
