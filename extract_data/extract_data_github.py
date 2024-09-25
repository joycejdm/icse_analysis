import requests
import pandas as pd
import time
from datetime import datetime
from collections import defaultdict

def get_paginated_data(url, headers):
    """
    A generic function to fetch paginated data from an API and handle rate limiting.
    
    Args:
        url (str): The API endpoint URL.
        headers (dict): Headers for authentication and other request settings.
    
    Returns:
        list: Collected data from all pages.
    """
    items = []
    while url:
        response = requests.get(url, headers=headers)
        
        # Check the rate limit and handle if it's near the limit
        if 'X-RateLimit-Remaining' in response.headers:
            remaining = int(response.headers['X-RateLimit-Remaining'])
            reset_time = int(response.headers['X-RateLimit-Reset'])
            if remaining < 10:  # Low request limit threshold
                sleep_time = reset_time - int(time.time()) + 10  # Wait until the reset
                print(f"Rate limit reached. Waiting {sleep_time} seconds...")
                time.sleep(sleep_time)

        if response.status_code != 200:
            raise Exception(f"Error accessing the API: {response.json()}")
        
        data = response.json()
        items.extend(data)
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            break
    return items

def get_repos(org_name, token):
    """
    Retrieves all repositories for a given organization.
    
    Args:
        org_name (str): Organization's GitHub name.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of repository names.
    """
    repos_url = f"https://api.github.com/orgs/{org_name}/repos"
    headers = {'Authorization': f'token {token}'}
    repos = get_paginated_data(repos_url, headers)
    
    # Return all repository names
    repo_names = [repo['name'] for repo in repos]
    return repo_names

def get_contributors(org_name, repo_name, token):
    """
    Retrieves all contributors of a specific repository.
    
    Args:
        org_name (str): Organization's GitHub name.
        repo_name (str): Repository name.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of contributors for the repository.
    """
    contributors_url = f"https://api.github.com/repos/{org_name}/{repo_name}/contributors"
    headers = {'Authorization': f'token {token}'}
    return get_paginated_data(contributors_url, headers)

def get_commits(org_name, repo_name, user_login, token):
    """
    Retrieves all commits made by a specific user in a repository.
    
    Args:
        org_name (str): Organization's GitHub name.
        repo_name (str): Repository name.
        user_login (str): User's GitHub login.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of commits made by the user in the repository.
    """
    commits_url = f"https://api.github.com/repos/{org_name}/{repo_name}/commits?author={user_login}&per_page=100"
    headers = {'Authorization': f'token {token}'}
    return get_paginated_data(commits_url, headers)

def get_pull_requests(org_name, repo_name, user_login, token):
    """
    Retrieves all pull requests made by a specific user in a repository.
    
    Args:
        org_name (str): Organization's GitHub name.
        repo_name (str): Repository name.
        user_login (str): User's GitHub login.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of pull requests made by the user.
    """
    pulls_url = f"https://api.github.com/repos/{org_name}/{repo_name}/pulls?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    pulls = get_paginated_data(pulls_url, headers)
    return [pr for pr in pulls if pr['user']['login'] == user_login]

def get_issues(org_name, repo_name, user_login, token):
    """
    Retrieves all issues created by a specific user in a repository.
    
    Args:
        org_name (str): Organization's GitHub name.
        repo_name (str): Repository name.
        user_login (str): User's GitHub login.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of issues created by the user.
    """
    issues_url = f"https://api.github.com/repos/{org_name}/{repo_name}/issues?state=all&per_page=100"
    headers = {'Authorization': f'token {token}'}
    issues = get_paginated_data(issues_url, headers)
    return [issue for issue in issues if issue['user']['login'] == user_login]

def get_reviews(org_name, repo_name, token):
    """
    Retrieves all pull request reviews from a specific repository.
    
    Args:
        org_name (str): Organization's GitHub name.
        repo_name (str): Repository name.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of reviews made in the repository.
    """
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
    """
    Retrieves the programming languages used in a specific repository.
    
    Args:
        org_name (str): Organization's GitHub name.
        repo_name (str): Repository name.
        token (str): Personal Access Token for GitHub API authentication.
    
    Returns:
        list: A list of programming languages used in the repository.
    """
    languages_url = f"https://api.github.com/repos/{org_name}/{repo_name}/languages"
    headers = {'Authorization': f'token {token}'}
    response = requests.get(languages_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching languages: {response.json()}")
    return list(response.json().keys())

def get_commit_history_by_month(commits):
    """
    Aggregates the commit history by month and year.
    
    Args:
        commits (list): A list of commits.
    
    Returns:
        dict: A dictionary with month-year as keys and the number of commits as values.
    """
    commit_history = defaultdict(int)
    for commit in commits:
        date_str = commit['commit']['author']['date']
        commit_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
        month_year = commit_date.strftime('%Y-%m')
        commit_history[month_year] += 1
    return commit_history

def save_progress(data_list, csv_file):
    """
    Saves the current progress to a CSV file.
    
    Args:
        data_list (list): Data to be saved.
        csv_file (str): The CSV file path where data will be saved.
    """
    df = pd.DataFrame(data_list)
    df.to_csv(csv_file, index=False)

def load_existing_data(csv_file):
    """
    Loads existing data from a CSV file if it exists.
    
    Args:
        csv_file (str): The CSV file path to load data from.
    
    Returns:
        list: Loaded data as a list of dictionaries.
    """
    try:
        return pd.read_csv(csv_file).to_dict(orient='records')
    except FileNotFoundError:
        return []

# Authentication and configuration
token = 'your_github_token_here'  # Replace with your GitHub token
org = 'your_organization_name_here'  # Replace with the desired organization name
csv_file = 'github_repo_data_complete.csv'  # CSV file for saving progress
repo_data_list = load_existing_data(csv_file)  # Load existing progress

# Identify repositories already processed
processed_repos = set([repo['repo'] for repo in repo_data_list])

# Get all repositories of the organization
repos = get_repos(org, token)

# Main processing loop for each repository and its contributors
for repo_name in repos:
    if repo_name in processed_repos:
        print(f"Repository {repo_name} already processed. Skipping.")
        continue  # Skip this repository and move to the next

    print(f"Extracting data from repository: {repo_name}")
    
    # Fetch contributors and languages
    contributors = get_contributors(org, repo_name, token)
    languages = get_languages(org, repo_name, token)
    
    # For each contributor, gather data
    for contributor in contributors:
        user_login = contributor['login']
        
        # Collect information per user
        user_commits = get_commits(org, repo_name, user_login, token)
        commit_history = get_commit_history_by_month(user_commits)
        pull_requests = get_pull_requests(org, repo_name, user_login, token)
        issues = get_issues(org, repo_name, user_login, token)
        reviews = [review for review in get_reviews(org, repo_name, token) if review['user']['login'] == user_login]
        
        # Add data to the dataset
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

    # Save progress after each repository is processed
    save_progress(repo_data_list, csv_file)
    print(f"Progress saved for repository: {repo_name}")

print("Processing completed!")
