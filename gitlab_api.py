import csv
import os
import requests

from datetime import datetime as dt

BASE_URL = os.environ.get('BASE_URL')
API_URL = '{}/api/v4/'.format(BASE_URL)
HEADERS = {
    'PRIVATE-TOKEN': os.environ.get('PRIVATE_ACCESS_TOKEN'),
}
REQUIRED_FIELDS = [
    'committed_date',
    'committer_name',
    'title',
    # full list of possible required fields can be found in the README
]

DATE_FORMAT = '%Y-%m-%d'
TARGET_YEAR = 2017
TARGET_PERIOD = {
    'from': '2017-01-01',
    'to': '2017-12-31',
}
START_DATE = dt.strptime(TARGET_PERIOD['from'], DATE_FORMAT)
END_DATE = dt.strptime(TARGET_PERIOD['to'], DATE_FORMAT)
TARGET_FILE_PATH = 'gitlab_commits_{}.csv'.format(TARGET_YEAR)

project_ids = []
try:
    with open('project_details.csv') as _csv:
        rows = csv.reader(_csv, delimiter=',')
        project_ids = []
        for row in rows:
            _id = int(row[0])
            name = row[1]
            project_ids.append({
                'id': _id,
                'name': name,
            })
except FileNotFoundError as err:
    print("""
        * run `gitlab-rails dbconsole` on your gitlab server
        * once in psql run: `\copy (select id, name from projects) to '/tmp/project_details.csv' with delimiter ',';`
        * copy this file to this working directory
    """)
    raise


def get_required_fields(commit_objs=[], required_fields=[]):
    # this is only needed if you don't want all fields from gitlab commits
    temp_commit_objs = []
    for obj in commit_objs:
        obj['message'] = obj['message'].strip()
        commit_date = dt.strptime(obj['committed_date'][0:10], DATE_FORMAT)
        if START_DATE < commit_date < END_DATE:
            temp_commit_objs.append({
                key:obj[key]
                for key in obj
                if key in required_fields
            })
    return temp_commit_objs


def get_all_project_commits():
    commits = {}
    projects_api_url = "{}/projects/".format(API_URL)
    s = requests.Session()
    for project in project_ids:
        params = {
            "per_page": 100,
            "page": 1,
        }
        response = s.get(
            "{}/{}/repository/commits".format(projects_api_url, project['id']), 
            headers=HEADERS, 
            params=params,
        )
        if len(REQUIRED_FIELDS) == 0: # for all fields
            results = [obj for obj in response.json()]
        else:
            results = get_required_fields(response.json(), REQUIRED_FIELDS)
        # XXX needed for pagination of gitlab api
        while params['page'] < int(response.headers['x-total-pages']):
            params['page'] += 1
            response = s.get(
                "{}/{}/repository/commits".format(projects_api_url, project['id']), 
                headers=HEADERS, 
                params=params
            )
            if len(REQUIRED_FIELDS) == 0:
                results += [obj for obj in response.json()]
            else:
                results += get_required_fields(response.json(), REQUIRED_FIELDS)
        if len(results) > 0:
            commits[project['name']] = results
    return commits


def send_commits_to_file(header=[], commits={}):
    with open(TARGET_FILE_PATH, 'w') as _csv:
        fw = csv.writer(
            _csv,
            delimiter=',',
            quoting=csv.QUOTE_MINIMAL,
        )
        fw.writerow(header)
        for key in commits.keys():
            for row in commits[key]:
                # _row = [key] + [row]
                _row = [key, row['committer_name'], row['committed_date'], row['title']] # to ensure correct order
                fw.writerow(_row)


header = ['Project name', 'Developer', 'Commit date', 'Commit message']
commits = get_all_project_commits()
send_commits_to_file(header, commits)

