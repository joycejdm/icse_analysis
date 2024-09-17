import requests
import pandas as pd
from datetime import datetime

def get_paginated_data(url, headers):
    """Função genérica para coletar dados com paginação."""
    items = []
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar a API: {response.json()}")
        data = response.json()
        items.extend(data)
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            break
    return items

def get_repos(org_name, token, start_year=2017):
    """Lista todos os repositórios da organização que começam com um padrão de ano e semestre."""
    repos_url = f"https://api.github.com/orgs/{org_name}/repos"
    headers = {'Authorization': f'token {token}'}
    repos = get_paginated_data(repos_url, headers)
    current_year = datetime.now().year

    filtered_repos = []
    for repo in repos:
        repo_name = repo['name']
        # Verifica se o nome segue o padrão de ano (AAAA.X) e ignora outros formatos
        try:
            year = int(repo_name.split('.')[0])
            if start_year <= year <= current_year:
                filtered_repos.append(repo_name)
        except ValueError:
            # Ignora repositórios que não têm um ano no início do nome
            continue

    return filtered_repos

def get_contributors(org_name, repo_name, token):
    """Lista os contribuidores de um repositório específico."""
    contributors_url = f"https://api.github.com/repos/{org_name}/{repo_name}/contributors"
    headers = {'Authorization': f'token {token}'}
    return get_paginated_data(contributors_url, headers)

def get_commits(org_name, repo_name, user_login, token):
    """Lista todos os commits de um usuário em um repositório específico."""
    commits_url = f"https://api.github.com/repos/{org_name}/{repo_name}/commits?author={user_login}&per_page=100"
    headers = {'Authorization': f'token {token}'}
    return get_paginated_data(commits_url, headers)

def get_pull_requests(org_name, repo_name, token):
    """Lista todos os pull requests de um repositório."""
    pulls_url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    return get_paginated_data(pulls_url, headers)

def get_issues(org_name, repo_name, token):
    """Lista todas as issues de um repositório."""
    issues_url = f"https://api.github.com/repos/{org_name}/{repo_name}/issues?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    return get_paginated_data(issues_url, headers)

# Autenticação e configuração
token = 'token'
org = 'fga-eps-mds'
repo_data_list = []
repos = get_repos(org, token)

# Processamento principal para cada repositório e seus contribuidores
for repo_name in repos:
    contributors = get_contributors(org, repo_name, token)
    for contributor in contributors:
        user_login = contributor['login']
        user_commits = get_commits(org, repo_name, user_login, token)
        pull_requests = get_pull_requests(org, repo_name, token)
        issues = get_issues(org, repo_name, token)
        repo_data_list.append({
            'org': org,
            'repo': repo_name,
            'user': user_login,
            'commits': len(user_commits),
            'pull_requests': len(pull_requests),
            'issues': len(issues)
        })

# Criação do DataFrame e salvamento dos dados
repo_data = pd.DataFrame(repo_data_list)
print(repo_data)
repo_data.to_csv('github_repo_data_complete_fixed.csv', index=False)
