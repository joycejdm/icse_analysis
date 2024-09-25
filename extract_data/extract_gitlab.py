import requests
import pandas as pd
import time
from datetime import datetime
from collections import defaultdict
from dateutil import parser

def get_paginated_data(url, headers):
    """Função genérica para coletar dados com paginação e controle de limite de requisições."""
    items = []
    while url:
        response = requests.get(url, headers=headers)
        
        if 'RateLimit-Remaining' in response.headers:
            remaining = int(response.headers['RateLimit-Remaining'])
            reset_time = int(response.headers['RateLimit-Reset'])
            if remaining < 10:  
                sleep_time = reset_time - int(time.time()) + 10  
                print(f"Limite de requisições atingido. Esperando {sleep_time} segundos...")
                time.sleep(sleep_time)

        if response.status_code == 403:
            print(f"Erro 403: Acesso negado ao URL: {url}")
            return []  # Retorna lista vazia para evitar quebra do fluxo
        
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar a API: {response.json()}")
        
        data = response.json()
        items.extend(data)
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            break
    return items

def get_subgroups_and_projects(group_id, token):
    """Retorna todos os subgrupos e os projetos de um grupo (recursivamente em subgrupos)."""
    all_projects = []
    headers = {'Authorization': f'Bearer {token}'}
    
    subgroups_url = f"https://gitlab.com/api/v4/groups/{group_id}/subgroups"
    subgroups = get_paginated_data(subgroups_url, headers)
    
    projects_url = f"https://gitlab.com/api/v4/groups/{group_id}/projects"
    projects = get_paginated_data(projects_url, headers)
    all_projects.extend(projects)
    
    for subgroup in subgroups:
        print(f"Coletando projetos do subgrupo: {subgroup['name']}")
        all_projects.extend(get_subgroups_and_projects(subgroup['id'], token))
    
    return all_projects

def get_contributors(project_id, token):
    """Lista os contribuidores de um projeto específico."""
    contributors_url = f"https://gitlab.com/api/v4/projects/{project_id}/repository/contributors"
    headers = {'Authorization': f'Bearer {token}'}
    return get_paginated_data(contributors_url, headers)

def get_commits(project_id, user_login, token):
    """Lista todos os commits de um usuário em um projeto específico."""
    commits_url = f"https://gitlab.com/api/v4/projects/{project_id}/repository/commits?author={user_login}"
    headers = {'Authorization': f'Bearer {token}'}
    return get_paginated_data(commits_url, headers)

def get_languages(project_id, token):
    """Extrai as linguagens utilizadas em um projeto."""
    languages_url = f"https://gitlab.com/api/v4/projects/{project_id}/languages"
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(languages_url, headers=headers)
    if response.status_code == 403:
        print(f"Erro 403: Acesso negado às linguagens do projeto {project_id}")
        return []
    if response.status_code != 200:
        raise Exception(f"Erro ao buscar linguagens: {response.json()}")
    return list(response.json().keys())

def get_commit_history_by_month(commits):
    """Agrupa os commits por mês e ano."""
    commit_history = defaultdict(int)
    for commit in commits:
        date_str = commit['created_at']
        commit_date = parser.isoparse(date_str)
        month_year = commit_date.strftime('%Y-%m')
        commit_history[month_year] += 1
    return commit_history

def save_progress(data_list, csv_file):
    """Salva o progresso atual no arquivo CSV."""
    df = pd.DataFrame(data_list)
    df.to_csv(csv_file, index=False)

def load_existing_data(csv_file):
    """Carrega os dados já extraídos, se o arquivo existir."""
    try:
        return pd.read_csv(csv_file).to_dict(orient='records')
    except FileNotFoundError:
        return []

def get_merge_requests(project_id, token):
    """Coleta todos os merge requests de um projeto sem filtrar por autor."""
    mrs_url = f"https://gitlab.com/api/v4/projects/{project_id}/merge_requests?state=all"
    headers = {'Authorization': f'Bearer {token}'}
    return get_paginated_data(mrs_url, headers)

def get_issues(project_id, token):
    """Coleta todas as issues de um projeto sem filtrar por autor."""
    issues_url = f"https://gitlab.com/api/v4/projects/{project_id}/issues?state=all"
    headers = {'Authorization': f'Bearer {token}'}
    return get_paginated_data(issues_url, headers)

def combine_mrs_and_issues(contributors, merge_requests, issues):
    """Adiciona MRs e issues ao contribuidor correto com base no 'username', 'name' ou 'email'."""
    for contributor in contributors:
        contributor_name = contributor['name'].strip().lower()  # Normalize name
        contributor_email = contributor['email'].strip().lower()  # Normalize email
        contributor_username = contributor.get('username', '').strip().lower()  # Use o 'username' para comparação

        # Contagem de merge requests associadas ao contribuidor
        mr_count = len([
            mr for mr in merge_requests 
            if mr['author']['username'].strip().lower() == contributor_username or
               mr['author']['name'].strip().lower() == contributor_name or 
               mr['author']['username'].strip().lower() == contributor_email
        ])
        
        # Contagem de issues associadas ao contribuidor
        issue_count = len([
            issue for issue in issues 
            if issue['author']['username'].strip().lower() == contributor_username or
               issue['author']['name'].strip().lower() == contributor_name or 
               issue['author']['username'].strip().lower() == contributor_email
        ])
        
        # Atualiza os valores de MR e issues para o contribuidor correspondente
        contributor['merge_requests'] = mr_count
        contributor['issues'] = issue_count

token = 'token'
group_ids = [2273686]  
csv_file = 'gitlab_repo_data_complete.csv'
repo_data_list = load_existing_data(csv_file)  

processed_projects = set([repo['project'] for repo in repo_data_list if 'project' in repo])

for group_id in group_ids:
    print(f"Processando grupo: {group_id}")
    
    all_projects = get_subgroups_and_projects(group_id, token)

    for project in all_projects:
        project_id = project['id']
        if project['name'] in processed_projects:
            print(f"Projeto {project['name']} já processado. Pulando.")
            continue 
            
        print(f"Extraindo dados do projeto: {project['name']}")
        
        try:
            contributors = get_contributors(project_id, token)
            languages = get_languages(project_id, token)
        
        except Exception as e:
            print(f"Erro ao buscar dados do projeto {project['name']}: {e}")
            continue
        
        contributors_data = []
        
        for contributor in contributors:
            try:
                user_commits = get_commits(project_id, contributor['name'], token)
                commit_history = get_commit_history_by_month(user_commits)

                contributors_data.append({
                    'group': project.get('namespace', {}).get('full_path', 'N/A'),  
                    'project_id': project_id,  
                    'project': project['name'],
                    'name': contributor['name'],  
                    'email': contributor['email'],  
                    'commits': len(user_commits),
                    'commit_history': dict(commit_history),
                    'languages': ', '.join(languages),
                    'merge_requests': 0,  
                    'issues': 0  
                })

            except Exception as e:
                print(f"Erro ao buscar dados para o usuário {contributor['name']}: {e}")
                continue

        merge_requests = get_merge_requests(project_id, token)
        issues = get_issues(project_id, token)

        combine_mrs_and_issues(contributors_data, merge_requests, issues)
        
        repo_data_list.extend(contributors_data)

        save_progress(repo_data_list, csv_file)
        print(f"Progresso salvo para o projeto: {project['name']}")

print("Processamento concluído!")
