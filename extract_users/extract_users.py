import requests
import pandas as pd
import time
from datetime import datetime
from collections import defaultdict

def get_paginated_data(url, headers):
    """Função genérica para coletar dados com paginação e controle de limite de requisições."""
    items = []
    while url:
        response = requests.get(url, headers=headers)
        
        # Verifica o limite de requisições
        if 'X-RateLimit-Remaining' in response.headers:
            remaining = int(response.headers['X-RateLimit-Remaining'])
            reset_time = int(response.headers['X-RateLimit-Reset'])
            if remaining < 10:  # Limite baixo de requisições
                sleep_time = reset_time - int(time.time()) + 10  # Espera até reset
                print(f"Limite de requisições atingido. Esperando {sleep_time} segundos...")
                time.sleep(sleep_time)

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
        try:
            year = int(repo_name.split('.')[0])
            if start_year <= year <= current_year:
                filtered_repos.append(repo_name)
        except ValueError:
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

def get_pull_requests(org_name, repo_name, user_login, token):
    """Lista todos os pull requests de um usuário em um repositório."""
    pulls_url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    pulls = get_paginated_data(pulls_url, headers)
    return [pr for pr in pulls if pr['user']['login'] == user_login]

def get_issues(org_name, repo_name, user_login, token):
    """Lista todas as issues de um usuário em um repositório."""
    issues_url = f"https://api.github.com/repos/{org_name}/{repo_name}/issues?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    issues = get_paginated_data(issues_url, headers)
    return [issue for issue in issues if issue['user']['login'] == user_login]

def get_reviews(org_name, repo_name, token):
    """Lista todos os reviews de pull requests em um repositório."""
    pulls_url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    pulls = get_paginated_data(pulls_url, headers)
    reviews = []
    for pr in pulls:
        review_url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls/{pr['number']}/reviews"
        pr_reviews = get_paginated_data(review_url, headers)
        reviews.extend(pr_reviews)
    return reviews

def get_languages(org_name, repo_name, token):
    """Extrai as linguagens e frameworks utilizados em um repositório."""
    languages_url = f"https://api.github.com/repos/{org_name}/{repo_name}/languages"
    headers = {'Authorization': f'token {token}'}
    response = requests.get(languages_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Erro ao buscar linguagens: {response.json()}")
    return list(response.json().keys())

def get_commit_history_by_month(commits):
    """Agrupa os commits por mês e ano."""
    commit_history = defaultdict(int)
    for commit in commits:
        date_str = commit['commit']['author']['date']
        commit_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
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

# Autenticação e configuração
token = 'token'
org = 'fga-eps-mds'
csv_file = 'github_repo_data_complete_with_logs.csv'
repo_data_list = load_existing_data(csv_file)  # Carregar progresso existente

# Identifica quais repositórios já foram processados
processed_repos = set([repo['repo'] for repo in repo_data_list])

# Obtém todos os repositórios da organização
repos = get_repos(org, token)

# Processamento principal para cada repositório e seus contribuidores
for repo_name in repos:
    if repo_name in processed_repos:
        print(f"Repositório {repo_name} já processado. Pulando.")
        continue  # Pular este repositório e ir para o próximo

    print(f"Extraindo dados do repositório: {repo_name}")
    
    # Coleta contribuidores e linguagens
    contributors = get_contributors(org, repo_name, token)
    languages = get_languages(org, repo_name, token)
    
    # Para cada contribuidor, coleta dados
    for contributor in contributors:
        user_login = contributor['login']
        
        # Coletar informações por usuário
        user_commits = get_commits(org, repo_name, user_login, token)
        commit_history = get_commit_history_by_month(user_commits)
        pull_requests = get_pull_requests(org, repo_name, user_login, token)
        issues = get_issues(org, repo_name, user_login, token)
        reviews = [review for review in get_reviews(org, repo_name, token) if review['user']['login'] == user_login]
        
        # Adicionar dados ao dataset
        repo_data_list.append({
            'org': org,
            'repo': repo_name,
            'user': user_login,
            'commits': len(user_commits),
            'commit_history': dict(commit_history),
            'pull_requests': len(pull_requests),
            'issues': len(issues),
            'reviews': len(reviews),
            'languages': ', '.join(languages)
        })

    # Salvar progresso a cada repositório processado
    save_progress(repo_data_list, csv_file)
    print(f"Progresso salvo para o repositório: {repo_name}")

print("Processamento concluído!")
