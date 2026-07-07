import subprocess
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

PROJECT_DIR = '/opt/dbt/hw1'
PROFILES_DIR = '/opt/airflow/.dbt'

def run_dbt_command(command: str, project_dir: str) -> Dict[str, Any]:
    """Выполнение dbt команды и возврат результатов"""
    # формирование команды
    target = 'dev';
    cmd = [
        'dbt', command,
        '--project-dir', project_dir,
        '--profiles-dir', PROFILES_DIR,
        '--target', target,
    ]
    
    # отладочные флаги
    cmd.append('--debug')  # Для детального вывода
    
    # Добавляем переменные
    
    logging.info("=" * 80)
    logging.info(f"COMMAND: {' '.join(cmd)}")
    logging.info("=" * 80)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return {
            'success': True,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.CalledProcessError as e:
        return {
            'success': False,
            'stdout': e.stdout,
            'stderr': e.stderr,
            'return_code': e.returncode
        }


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
        with open(results_file, 'r') as f:
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
                    stats['rows_processed'][node.get('name')] = node.get('config', {}).get('meta', {}).get('row_count', 0)
    
    return stats


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


    @task
    def run_dbt_and_collect_stats(project_dir: str):
        result = run_dbt_command('run', project_dir)
    
        if not result['success']:
            raise Exception(f"dbt run failed: {result['stderr']}")
        
        # 2. Парсинг результатов
        stats = parse_dbt_results(project_dir)
        stats['run_timestamp'] = datetime.now()
    
    # 3. Сохранение в XCom для использования в других задачах
        return stats
    
    @task
    def run_dbt_tests(project_dir: str) -> Dict[str, Any]:
        """Запуск dbt тестов"""
        result = run_dbt_command('test', project_dir)
        
        if not result['success']:
            # Логируем ошибки, но не останавливаем DAG
            logging.error(f"dbt test failed: {result['stderr']}")
            result = False
        else:
            result = True
        return {'result': result}
        
   
    
    @task
    def send_alert(stats: Dict[str, Any], tests_passed: bool):
        """Отправка алерта с статистикой"""
        
        #сообщение
        message = f"""
        DBT Run Report
        ==============
        
        Overview:
        - Total Models: {stats.get('total_models', 0)}
        - Successful: {stats.get('successful_models', 0)}
        - Failed: {stats.get('failed_models', 0)}
        - Skipped: {stats.get('skipped_models', 0)}
        - Total Execution Time: {stats.get('execution_time', 0):.2f}s
        
        Tests:
        - Total Tests: {stats.get('total_tests', 0)}
        - Passed: {stats.get('passed_tests', 0)}
        - Failed: {stats.get('failed_tests', 0)}
        - Tests Overall Status: {'✅ PASSED' if tests_passed['result'] else '❌ FAILED'}
        
        Model Performance (Top 5 slowest):
        """
        
        # Добавление информации о производительности моделей
        if stats.get('models_performance'):
            sorted_models = sorted(
                stats['models_performance'],
                key=lambda x: x['execution_time'],
                reverse=True
            )[:5]
            
            for model in sorted_models:
                message += f"\n  - {model['model']}: {model['execution_time']:.2f}s, Rows: {model.get('rows_affected', 0)}, Status: {model.get('status', 'unknown')}"
        
        # Отправка email не получилась, так как делаю с рабочего компьютера
        print(message)

    
    

    dbt_run_task = run_dbt_and_collect_stats(PROJECT_DIR)

    dbt_test_task = run_dbt_tests(PROJECT_DIR)

    send_alerts = send_alert(dbt_run_task, dbt_test_task)
    

    dbt_run_task >> dbt_test_task >> send_alerts


dbt_pipeline()
