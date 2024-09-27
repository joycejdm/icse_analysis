import requests
import csv
import time
from typing import List, Set, Dict, Any, Optional

class GitHubContributors:
    def __init__(self, token: str, organization: str, start_date: str, end_date: str, output_csv: str):
        """
        Inicializa a classe com as credenciais de autenticação do GitHub e outros parâmetros.

        Args:
            token (str): O token de autenticação do GitHub.
            organization (str): A organização no GitHub.
            start_date (str): Data de início no formato ISO 8601.
            end_date (str): Data de término no formato ISO 8601.
            output_csv (str): Nome do arquivo CSV de saída.
        """
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }
        self.organization = organization
        self.start_date = start_date
        self.end_date = end_date
        self.output_csv = output_csv

    def get_repositories(self) -> List[str]:
        """
        Obtém a lista de repositórios da organização.

        Returns:
            List[str]: Lista de nomes de repositórios.
        """
        repos = []
        url = f'https://api.github.com/orgs/{self.organization}/repos'
        while url:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            repos.extend(response.json())
            url = response.links.get('next', {}).get('url')
        return [repo['name'] for repo in repos]

    def get_commits(self, repo: str) -> List[Dict[str, Any]]:
        """
        Obtém a lista de commits de um repositório específico.

        Args:
            repo (str): Nome do repositório.

        Returns:
            List[Dict[str, Any]]: Lista de commits.
        """
        commits = []
        url = f'https://api.github.com/repos/{self.organization}/{repo}/commits'
        params = {
            'since': self.start_date,
            'until': self.end_date,
            'per_page': 100,
        }
        while url:
            attempts = 0
            success = False
            while attempts < 2 and not success:
                try:
                    response = requests.get(url, headers=self.headers, params=params)
                    response.raise_for_status()
                    commits.extend(response.json())
                    url = response.links.get('next', {}).get('url')
                    success = True
                except requests.exceptions.RequestException as e:
                    attempts += 1
                    print(f"Erro ao obter commits do repositório {repo}: {e}")
                    print(f"Tentando novamente ({attempts}/2)...")
                    time.sleep(5)  # Espera 5 segundos antes de tentar novamente
                    if attempts == 2:
                        print("Máximo de tentativas alcançado. Pulando para o próximo repositório.")
                        url = None
        return commits

    @staticmethod
    def get_contributors(commits: List[Dict[str, Any]]) -> Set[str]:
        """
        Obtém a lista de contribuidores a partir da lista de commits.

        Args:
            commits (List[Dict[str, Any]]): Lista de commits.

        Returns:
            Set[str]: Conjunto de logins de contribuidores.
        """
        contributors = set()
        for commit in commits:
            if commit['author']:  # Verifica se o commit tem um autor associado
                contributors.add(commit['author']['login'])
        return contributors

    def save_to_csv(self, contributors: Set[str]):
        """
        Salva a lista de contribuidores em um arquivo CSV.

        Args:
            contributors (Set[str]): Conjunto de logins de contribuidores.
        """
        with open(self.output_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['User'])
            for contributor in contributors:
                writer.writerow([contributor])
        print(f'Successfully written to {self.output_csv}')

    def run(self):
        """
        Executa o processo completo de obtenção de contribuidores e salvamento em CSV.
        """
        all_contributors = set()
        repositories = self.get_repositories()
        for repo in repositories:
            print(f'Processing repository: {repo}')
            commits = self.get_commits(repo)
            contributors = self.get_contributors(commits)
            all_contributors.update(contributors)
        self.save_to_csv(all_contributors)

if __name__ == '__main__':
    # Substitua esses valores pelos parâmetros desejados
    token = 'token'
    organization = 'organization'
    start_date = '2017-01-01T00:00:00Z'
    end_date = '2024-07-01T23:59:59Z'
    output_csv = 'output_csv.csv'

    github_contributors = GitHubContributors(token, organization, start_date, end_date, output_csv)
    github_contributors.run()