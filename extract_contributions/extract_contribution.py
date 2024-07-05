import requests
import csv
import datetime
from collections import defaultdict
from typing import Dict, Any, List, Optional

class GitHubUserData:
    def __init__(self, token: str):
        """
        Inicializa a classe com o token de autenticação do GitHub.

        Args:
            token (str): O token de autenticação do GitHub.
        """
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def get_user_data(self, user: str) -> Dict[str, Any]:
        """
        Obtém dados do usuário do GitHub usando a API GraphQL.

        Args:
            user (str): O nome de usuário do GitHub.

        Returns:
            Dict[str, Any]: Um dicionário contendo dados do usuário.
        """
        query = """
        query($user: String!) {
          user(login: $user) {
            contributionsCollection(from: "2023-06-01T00:00:00Z", to: "2024-06-01T23:59:59Z") {
              contributionCalendar {
                totalContributions
                weeks {
                  contributionDays {
                    date
                    contributionCount
                  }
                }
              }
              commitContributionsByRepository {
                contributions(first: 100) {
                  totalCount
                }
              }
              pullRequestContributionsByRepository {
                contributions(first: 100) {
                  totalCount
                }
              }
              issueContributionsByRepository {
                contributions(first: 100) {
                  totalCount
                }
              }
              pullRequestReviewContributionsByRepository {
                contributions(first: 100) {
                  totalCount
                }
              }
            }
            repositories(first: 100) {
              totalCount
              nodes {
                primaryLanguage {
                  name
                }
              }
            }
          }
        }
        """
        
        variables = {
            "user": user
        }
        
        url = "https://api.github.com/graphql"
        response = requests.post(url, json={'query': query, 'variables': variables}, headers=self.headers)
        
        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                print(f"Erro ao acessar dados do usuário {user}: {data['errors']}")
                return self._empty_user_data()
            
            user_data = data.get("data", {}).get("user", {})
            if not user_data:
                print(f"Usuário não encontrado ou sem dados: {user}")
                return self._empty_user_data()
            
            return self._parse_user_data(user_data)
        else:
            print(f"Erro ao acessar dados do usuário {user}: {response.status_code} {response.text}")
            return self._empty_user_data()

    def _parse_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analisa os dados do usuário obtidos da API do GitHub.

        Args:
            user_data (Dict[str, Any]): Dados brutos do usuário do GitHub.

        Returns:
            Dict[str, Any]: Dados analisados do usuário.
        """
        contributions = user_data.get("contributionsCollection", {}).get("contributionCalendar", {}).get("totalContributions", 0)
        repositories = user_data.get("repositories", {}).get("totalCount", 0)
        
        languages = {}
        for repo in user_data.get("repositories", {}).get("nodes", []):
            primary_language = repo.get("primaryLanguage")
            if primary_language is not None:
                language = primary_language.get("name")
                if language:
                    languages[language] = languages.get(language, 0) + 1
        
        primary_language = max(languages, key=languages.get) if languages else "N/A"
        
        weekly_contributions = user_data.get("contributionsCollection", {}).get("contributionCalendar", {}).get("weeks", [])
        
        monthly_contributions = defaultdict(int)
        for week in weekly_contributions:
            for day in week.get("contributionDays", []):
                date = datetime.datetime.strptime(day["date"], "%Y-%m-%d")
                month = date.strftime("%Y-%m")
                monthly_contributions[month] += day["contributionCount"]
        
        contribution_types = {
            "commits": sum(repo["contributions"]["totalCount"] for repo in user_data.get("contributionsCollection", {}).get("commitContributionsByRepository", [])),
            "pull_requests": sum(repo["contributions"]["totalCount"] for repo in user_data.get("contributionsCollection", {}).get("pullRequestContributionsByRepository", [])),
            "issues": sum(repo["contributions"]["totalCount"] for repo in user_data.get("contributionsCollection", {}).get("issueContributionsByRepository", [])),
            "reviews": sum(repo["contributions"]["totalCount"] for repo in user_data.get("contributionsCollection", {}).get("pullRequestReviewContributionsByRepository", []))
        }
        
        return {
            "contributions": contributions,
            "repositories": repositories,
            "primary_language": primary_language,
            "monthly_contributions": dict(monthly_contributions),
            "contribution_types": contribution_types
        }

    @staticmethod
    def _empty_user_data() -> Dict[str, Any]:
        """
        Retorna um dicionário vazio para representar dados de usuário não encontrados ou com erro.

        Returns:
            Dict[str, Any]: Dicionário vazio com valores padrão.
        """
        return {
            "contributions": 0,
            "repositories": 0,
            "primary_language": "N/A",
            "monthly_contributions": {},
            "contribution_types": {}
        }

class CSVProcessor:
    def __init__(self, input_csv: str, output_csv: str):
        """
        Inicializa a classe com os nomes dos arquivos CSV de entrada e saída.

        Args:
            input_csv (str): Nome do arquivo CSV de entrada.
            output_csv (str): Nome do arquivo CSV de saída.
        """
        self.input_csv = input_csv
        self.output_csv = output_csv

    def read_users(self) -> List[str]:
        """
        Lê a lista de usuários do arquivo CSV de entrada.

        Returns:
            List[str]: Lista de nomes de usuários do GitHub.
        """
        with open(self.input_csv, mode='r', newline='') as file:
            reader = csv.reader(file)
            return [row[0] for row in reader]

    def write_summary(self, results: List[Dict[str, Any]]):
        """
        Escreve o resumo dos dados dos usuários no arquivo CSV de saída.

        Args:
            results (List[Dict[str, Any]]): Lista de dicionários contendo os dados dos usuários.
        """
        with open(self.output_csv, mode='w', newline='') as file:
            fieldnames = ["user", "contributions", "repositories", "primary_language", "monthly_contributions", "contribution_types"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                result["monthly_contributions"] = str(result["monthly_contributions"])
                result["contribution_types"] = str(result["contribution_types"])
                writer.writerow(result)
        print(f"Summary written to {self.output_csv}")

class GitHubContributorSummary:
    def __init__(self, token: str, input_csv: str, output_csv: str):
        """
        Inicializa a classe com o token de autenticação do GitHub, nomes dos arquivos CSV de entrada e saída.

        Args:
            token (str): O token de autenticação do GitHub.
            input_csv (str): Nome do arquivo CSV de entrada.
            output_csv (str): Nome do arquivo CSV de saída.
        """
        self.github_user_data = GitHubUserData(token)
        self.csv_processor = CSVProcessor(input_csv, output_csv)

    def generate_summary(self):
        """
        Gera um resumo dos dados dos contribuidores do GitHub e escreve no arquivo CSV de saída.
        """
        users = self.csv_processor.read_users()
        results = []
        for user in users:
            user_data = self.github_user_data.get_user_data(user)
            results.append({
                "user": user,
                "contributions": user_data["contributions"],
                "repositories": user_data["repositories"],
                "primary_language": user_data["primary_language"],
                "monthly_contributions": user_data["monthly_contributions"],
                "contribution_types": user_data["contribution_types"]
            })
        self.csv_processor.write_summary(results)

if __name__ == '__main__':
    # Substitua esses valores pelos parâmetros desejados
    token = 'your_github_token'
    input_csv = 'users_github_lappis.csv'
    output_csv = 'contributors_summary_lappis.csv'

    summary_generator = GitHubContributorSummary(token, input_csv, output_csv)
    summary_generator.generate_summary()
