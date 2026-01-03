import requests
from requests.auth import HTTPBasicAuth
import csv
import base64
import json
from datetime import datetime


# ================= CONFIGURATION =================
# 1. Jira / Confluence Settings, global api token.
ATLASSIAN_WORKSPACE = ""
EMAIL_ADDRESS = ""
API_TOKEN = ""

# 2. Bitbucket Settings, scoped api token
# Permissions:
# read:account
# read:me
# read:project:bitbucket
# read:pullrequest:bitbucket
# read:repository:bitbucket
# read:user:bitbucket
# read:workspace:bitbucket
BITBUCKET_API_TOKEN = ""

# 3. Date Range
START_DATE = "2025-12-01"
END_DATE = "2026-01-06"

# 4. Data Filter (Emails & Names & Repositories)
DEPARTMENT_EMAILS = [
]

DEPARTMENT_NAMES = [
]

DEPARTMENT_REPOS = [
]
# =================================================


def get_auth_header():
    creds = f"{EMAIL_ADDRESS}:{API_TOKEN}"
    encoded = base64.b64encode(creds.encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}

def audit_jira():
    print("--- Auditing Jira ---")
    # Updated to search/jql endpoint as per deprecation notice
    url = f"https://{ATLASSIAN_WORKSPACE}.atlassian.net/rest/api/3/search/jql"
    # Calculate UTC times for JST range (UTC+9)
    # Start: 00:00 JST -> Previous Day 15:00 UTC
    # End: 23:59 JST -> Same Day 14:59 UTC
    from datetime import datetime, timedelta
    
    fmt = "%Y-%m-%d"
    dt_start = datetime.strptime(START_DATE, fmt)
    dt_end = datetime.strptime(END_DATE, fmt)
    
    # Adjust to UTC (Subtract 9 hours from JST times)
    # Start: 00:00 JST = (Date - 1 day) 15:00 UTC
    utc_start = dt_start - timedelta(hours=9)
    # End: 23:59:59 JST ~ (Date) 14:59 UTC
    # Actually 23:59 JST is 14:59 UTC. Let's cover the full day so 23:59:59.
    # 23:59 JST - 9h = 14:59 UTC
    utc_end = dt_end.replace(hour=23, minute=59) - timedelta(hours=9)
    
    jql_start = utc_start.strftime("%Y-%m-%d %H:%M")
    jql_end = utc_end.strftime("%Y-%m-%d %H:%M")
    
    jql = f'updated >= "{jql_start}" AND updated <= "{jql_end}" ORDER BY updated DESC'
    print(f"DEBUG JQL: {jql}")
    
    results = []
    next_page_token = None
    
    while True:
        payload = {
            "jql": jql,
            "fields": ["summary", "issuetype", "status", "updated", "assignee", "creator"],
            "maxResults": 100
        }
        if next_page_token:
            payload["nextPageToken"] = next_page_token

        # Using POST as GET /search can be restricted/deprecated for complex JQL
        response = requests.post(url, headers=get_auth_header(), json=payload)
        
        if response.status_code != 200:
            print(f"Error fetching Jira data: {response.text}")
            break
            
        data = response.json()
        issues = data.get("issues", [])
        if not issues:
            # Even if nextPageToken exists, if no issues, we might want to stop? 
            # Usually strict pagination continues until no token.
            pass
            
        for issue in issues:
            fields = issue["fields"]
            assignee = fields["assignee"]["displayName"] if fields.get("assignee") else "Unassigned"
            assignee_email = fields["assignee"].get("emailAddress") if fields.get("assignee") else None
            
            # Filter by Department (Email OR Name)
            if DEPARTMENT_EMAILS or DEPARTMENT_NAMES:
                match_email = assignee_email and assignee_email in DEPARTMENT_EMAILS
                match_name = assignee and assignee in DEPARTMENT_NAMES
                
                if not match_email and not match_name:
                    continue

            results.append([
                issue["key"],
                fields["issuetype"]["name"],
                fields["summary"],
                assignee,
                fields["status"]["name"],
                fields["updated"],
                f"https://{ATLASSIAN_WORKSPACE}.atlassian.net/browse/{issue['key']}"
            ])
        
        print(f"Fetched {len(issues)} Jira issues in this batch (Total saved: {len(results)})...")
        
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    # Save to CSV
    with open("audit_jira.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Key", "Type", "Summary", "Assignee", "Status", "Last Updated", "URL"])
        writer.writerows(results)
    print("Saved audit_jira.csv")

def audit_confluence():
    print("\n--- Auditing Confluence ---")
    url = f"https://{ATLASSIAN_WORKSPACE}.atlassian.net/wiki/rest/api/content/search"
    cql = f'lastmodified >= "{START_DATE}" AND lastmodified <= "{END_DATE}" ORDER BY lastmodified DESC'
    
    results = []
    seen_ids = set()

    # Initial params
    params = {
        "cql": cql,
        "limit": 50,
        "expand": "history.lastUpdated"
    }
    
    # We need the base URL for constructing next links
    # url defined above: https://{ATLASSIAN_WORKSPACE}.atlassian.net/wiki/rest/api/content/search
    # We need: https://{ATLASSIAN_WORKSPACE}.atlassian.net/wiki
    base_api_url = f"https://{ATLASSIAN_WORKSPACE}.atlassian.net/wiki"

    while True:
        print(f"Fetching Confluence pages...")
        try:
            response = requests.get(url, headers=get_auth_header(), params=params, timeout=60)
        except requests.exceptions.RequestException as e:
            print(f"Network error fetching Confluence data: {e}")
            break
        
        if response.status_code != 200:
            print(f"Error fetching Confluence data: {response.text}")
            break
            
        data = response.json()
        pages = data.get("results", [])
        if not pages:
            print("No more pages returned by API.")
            break
            
        # Infinite loop protection
        current_batch_ids = [p["id"] for p in pages]
        if any(pid in seen_ids for pid in current_batch_ids):
            print("Warning: API returned duplicate page IDs. Pagination loop detected. Stopping.")
            break
        seen_ids.update(current_batch_ids)

        for page in pages:
            history = page["history"]["lastUpdated"]
            author_name = history["by"]["displayName"]
            # Note: email might be hidden depending on privacy settings
            author_email = history["by"].get("email", "") 
            
            # Filter by Department (Email OR Name)
            if DEPARTMENT_EMAILS or DEPARTMENT_NAMES:
                match_email = author_email and author_email in DEPARTMENT_EMAILS
                match_name = author_name and author_name in DEPARTMENT_NAMES
                
                if not match_email and not match_name:
                    continue

            date = history["when"]
            results.append([
                page["id"],
                page["title"],
                page["type"],
                author_name,
                date,
                f"https://{ATLASSIAN_WORKSPACE}.atlassian.net/wiki{page['_links']['webui']}"
            ])
            
        print(f"Scanned {len(pages)} pages in this batch. Total matches found so far: {len(results)}")

        # Pagination logic using 'next' link
        links = data.get("_links", {})
        next_link = links.get("next")
        if not next_link:
            break
        
        # Set up for next iteration
        url = base_api_url + next_link
        params = None # Params are included in the next_link URL

    # Save to CSV
    with open("audit_confluence.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Title", "Type", "Last Updated By", "Date", "URL"])
        writer.writerows(results)
    print("Saved audit_confluence.csv")

def audit_bitbucket():
    print("\n--- Auditing Bitbucket ---")
    
    # 1. Get all repos in workspace
    repos_url = f"https://api.bitbucket.org/2.0/repositories/{ATLASSIAN_WORKSPACE}"
    repos = []
    # Set up authentication and headers
    auth = HTTPBasicAuth(EMAIL_ADDRESS, BITBUCKET_API_TOKEN)
    headers = {
        "Accept": "application/json"
    }
    
    print("Fetching repository list...")
    while repos_url:
        res = requests.get(repos_url, headers=headers, auth=auth)
        if res.status_code != 200:
            print(f"Error fetching repos: {res.status_code}, {res.text}")
            break
        data = res.json()
        repos.extend(data["values"])
        repos_url = data.get("next") # Pagination

    pr_data = []
    
    # 2. Get active PRs for each repo in range
    # Bitbucket API query for PRs uses updated_on
    q_param = f'updated_on >= "{START_DATE}T00:00:00+09:00" AND updated_on <= "{END_DATE}T23:59:59+09:00"'
    
    for repo in repos:
        slug = repo["slug"]
        # Filter by repository name
        if DEPARTMENT_REPOS:
            if slug not in DEPARTMENT_REPOS:
                continue

        pr_url = f"https://api.bitbucket.org/2.0/repositories/{ATLASSIAN_WORKSPACE}/{slug}/pullrequests"
        params = {"q": q_param, "pagelen": 50, "state": "ALL"}
        
        res = requests.get(pr_url, headers=headers, auth=auth, params=params)
        
        if res.status_code == 200:
            prs = res.json().get("values", [])
            for pr in prs:
                # PR author object is usually simpler than commit author
                # Try to get display_name
                author_obj = pr.get("author", {})
                author_name = author_obj.get("display_name", "Unknown")
                
                # Cannot easily get email from Bitbucket Cloud API v2 PR objects
                author_email = "" 

                # Filter by Department (Name only as email is not available)
                if DEPARTMENT_EMAILS or DEPARTMENT_NAMES:
                    match_name = author_name and author_name in DEPARTMENT_NAMES
                    if not match_name:
                        continue
                    
                pr_data.append([
                    repo["name"],
                    author_name,
                    pr["state"],
                    pr["updated_on"],
                    pr["title"].replace("\n", " ")[:100],
                    pr["links"]["html"]["href"]
                ])
            if prs:
                print(f"Found {len(prs)} PRs in {slug}")
        else:
            print(f"Could not access PRs for {slug}: {res.status_code}")

    # Save to CSV
    with open("audit_bitbucket.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Repository", "Author", "State", "Last Updated", "Title", "URL"])
        writer.writerows(pr_data)
    print("Saved audit_bitbucket.csv")


if __name__ == "__main__":
    # Uncomment the services you want to run
    #audit_jira()
    audit_confluence()
    #audit_bitbucket()
    print("\nAudit Complete.")
