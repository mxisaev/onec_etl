import requests
import sys

# Добавляем путь к Airflow
sys.path.append('/opt/airflow')

from airflow.models import Variable

def get_access_token():
    """Получаем access token для Power BI API"""
    tenant_id = Variable.get('powerbi_tenant_id')
    client_id = Variable.get('powerbi_client_id')
    client_secret = Variable.get('powerbi_client_secret')
    
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://analysis.windows.net/powerbi/api'
    }
    
    print(f"Получаем токен для tenant_id: {tenant_id}")
    
    resp = requests.post(token_url, data=token_data)
    resp.raise_for_status()
    return resp.json()['access_token']

def get_reports(access_token, workspace_id):
    """Получаем список отчётов из workspace"""
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    print(f"Получаем отчёты из workspace: {workspace_id}")
    
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()['value']

def get_datasets(access_token, workspace_id):
    """Получаем список датасетов из workspace"""
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    print(f"Получаем датасеты из workspace: {workspace_id}")
    
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()['value']

def main():
    """Основная функция"""
    print("=== Получение списка отчётов и датасетов Power BI ===")
    
    # Workspace ID из переменной Airflow
    workspace_id = Variable.get('powerbi_workspace_id')
    
    try:
        # Получаем токен
        access_token = get_access_token()
        print("✓ Токен получен успешно")
        
        # Получаем отчёты
        reports = get_reports(access_token, workspace_id)
        print(f"\n=== ОТЧЁТЫ ({len(reports)} шт.) ===")
        
        for i, report in enumerate(reports, 1):
            print(f"\n{i}. {report.get('name', 'N/A')}")
            print(f"   ID: {report.get('id', 'N/A')}")
            print(f"   Dataset ID: {report.get('datasetId', 'N/A')}")
            print(f"   Embed URL: {report.get('embedUrl', 'N/A')}")
            print(f"   Web URL: {report.get('webUrl', 'N/A')}")
            print(f"   Created: {report.get('createdDateTime', 'N/A')}")
            print(f"   Modified: {report.get('modifiedDateTime', 'N/A')}")
        
        # Получаем датасеты
        datasets = get_datasets(access_token, workspace_id)
        print(f"\n=== ДАТАСЕТЫ ({len(datasets)} шт.) ===")
        
        for i, dataset in enumerate(datasets, 1):
            print(f"\n{i}. {dataset.get('name', 'N/A')}")
            print(f"   ID: {dataset.get('id', 'N/A')}")
            print(f"   Web URL: {dataset.get('webUrl', 'N/A')}")
            print(f"   Created: {dataset.get('createdDateTime', 'N/A')}")
            print(f"   Modified: {dataset.get('modifiedDateTime', 'N/A')}")
            print(f"   Tables: {len(dataset.get('tables', []))}")
            
            # Показываем таблицы в датасете
            tables = dataset.get('tables', [])
            if tables:
                print("   Таблицы:")
                for table in tables:
                    print(f"     - {table.get('name', 'N/A')}")
        
        print("\n=== ЗАВЕРШЕНО ===")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False
    
    return True

if __name__ == '__main__':
    main() 