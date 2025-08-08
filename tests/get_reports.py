import requests
import json
import os
import sys

# Добавляем путь к Airflow
sys.path.append('/opt/airflow')

from airflow.models import Variable

def get_access_token():
    """Получаем access token для Power BI API"""
    token_url = 'https://login.microsoftonline.com/common/oauth2/token'
    
    client_id = Variable.get('powerbi_client_id')
    client_secret = Variable.get('powerbi_client_secret')
    
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://analysis.windows.net/powerbi/api'
    }
    
    print(f"Получаем токен для client_id: {client_id}")
    
    token_response = requests.post(token_url, data=token_data)
    
    if token_response.status_code != 200:
        print(f"Ошибка получения токена: {token_response.status_code}")
        print(f"Ответ: {token_response.text}")
        return None
    
    token_json = token_response.json()
    print(f"Токен получен успешно")
    return token_json.get('access_token')

def get_reports(access_token):
    """Получаем список отчётов из workspace"""
    workspace_id = Variable.get('powerbi_workspace_id')
    reports_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    print(f"Получаем отчёты из workspace: {workspace_id}")
    
    reports_response = requests.get(reports_url, headers=headers)
    
    if reports_response.status_code != 200:
        print(f"Ошибка получения отчётов: {reports_response.status_code}")
        print(f"Ответ: {reports_response.text}")
        return None
    
    reports = reports_response.json()
    return reports.get('value', [])

def get_report_pages(access_token, report_id):
    """Получаем страницы отчёта"""
    workspace_id = Variable.get('powerbi_workspace_id')
    pages_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/pages'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    pages_response = requests.get(pages_url, headers=headers)
    
    if pages_response.status_code != 200:
        print(f"Ошибка получения страниц: {pages_response.status_code}")
        return None
    
    pages = pages_response.json()
    return pages.get('value', [])

def get_page_visuals(access_token, report_id, page_name):
    """Получаем визуализации страницы"""
    workspace_id = Variable.get('powerbi_workspace_id')
    visuals_url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/pages/{page_name}/visuals'
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    visuals_response = requests.get(visuals_url, headers=headers)
    
    if visuals_response.status_code != 200:
        print(f"Ошибка получения визуализаций: {visuals_response.status_code}")
        return None
    
    visuals = visuals_response.json()
    return visuals.get('value', [])

def main():
    print("=== Получение отчётов из Power BI ===")
    
    # Получаем токен
    access_token = get_access_token()
    if not access_token:
        print("Не удалось получить токен")
        return
    
    # Получаем отчёты
    reports = get_reports(access_token)
    if not reports:
        print("Не удалось получить отчёты")
        return
    
    print(f"\nНайдено отчётов: {len(reports)}")
    print("\n=== Список отчётов ===")
    
    for i, report in enumerate(reports, 1):
        print(f"\n{i}. Отчёт: {report.get('name', 'N/A')}")
        print(f"   ID: {report.get('id', 'N/A')}")
        print(f"   Embed URL: {report.get('embedUrl', 'N/A')}")
        
        # Получаем страницы отчёта
        pages = get_report_pages(access_token, report['id'])
        if pages:
            print(f"   Страниц: {len(pages)}")
            for j, page in enumerate(pages, 1):
                print(f"     {j}. {page.get('displayName', 'N/A')} (name: {page.get('name', 'N/A')})")
                
                # Получаем визуализации страницы
                visuals = get_page_visuals(access_token, report['id'], page['name'])
                if visuals:
                    print(f"       Визуализаций: {len(visuals)}")
                    for k, visual in enumerate(visuals, 1):
                        print(f"         {k}. {visual.get('name', 'N/A')} (type: {visual.get('type', 'N/A')})")
        
        print("   " + "-" * 50)

if __name__ == "__main__":
    main() 