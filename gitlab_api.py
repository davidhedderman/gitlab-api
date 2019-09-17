""" generate a csv file with commits pulled from gitlab api for a
specific time period
"""
import csv
import os

from datetime import datetime

import requests


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
START_DATE = datetime.strptime(TARGET_PERIOD['from'], DATE_FORMAT)
END_DATE = datetime.strptime(TARGET_PERIOD['to'], DATE_FORMAT)
TARGET_FILE_PATH = 'gitlab_commits_{}.csv'.format(TARGET_YEAR)


def get_project_ids(**params):
    """
    :param file_name:
    :return:
    :raises: FileNotFoundError
    """
    file_name = params.get('file_name')
    project_ids = []
    if file_name:
        try:
            with open(file_name) as csv_file:
                rows = csv.reader(
                    csv_file,
                    delimiter=','
                )
                for row in rows:
                    _id = int(row[0])
                    name = row[1]
                    project_ids.append({
                        'id': _id,
                        'name': name,
                    })
        except FileNotFoundError:
            print("""
                * run `gitlab-rails dbconsole` on your gitlab server
                * once in psql run: "\\copy (select id, name from projects) to '/tmp/project_details.csv' with delimiter ',';"
                * copy this file to this working directory
            """)
            raise
    return project_ids


def get_required_fields(commit_objs, required_fields):
    """
    :param commit_objs:
    :param required_fields:
    :return:
    """
    # this is only needed if you don't want all fields from gitlab commits
    temp_commit_objs = []
    for obj in commit_objs:
        obj['message'] = obj['message'].strip()
        commit_date = datetime.strptime(
            obj['committed_date'][0:10],
            DATE_FORMAT
        )
        if START_DATE < commit_date < END_DATE:
            temp_commit_objs.append({
                key: obj[key]
                for key in obj
                if key in required_fields
            })
    return temp_commit_objs


def get_project_commits_data(**params):
    """
    :return:
    """
    commits = {}
    projects_api_url = "{}/projects/".format(API_URL)
    session = requests.Session()
    project_ids = get_project_ids(file_name=params.get('file_name'))
    if project_ids:
        for project in project_ids:
            params = {
                "per_page": 100,
                "page": 1,
            }
            url = "{}/{}/repository/commits".format(projects_api_url, project['id'])
            response = session.get(url, headers=HEADERS, params=params)
            if not REQUIRED_FIELDS:  # for all fields
                results = [obj for obj in response.json()]
            else:
                results = get_required_fields(response.json(), REQUIRED_FIELDS)
            # needed for pagination of gitlab api
            while params['page'] < int(response.headers['x-total-pages']):
                params['page'] += 1
                response = session.get(url, headers=HEADERS, params=params)
                if not REQUIRED_FIELDS:
                    results += [obj for obj in response.json()]
                else:
                    results += get_required_fields(response.json(), REQUIRED_FIELDS)
            if results:
                commits[project['name']] = results
    return commits


def send_commits_to_file(**csv_params):
    """ create csv file

    :param header: array of str
    :param commits: dict of row data
    """
    header = csv_params.get('header', [])
    commits = csv_params.get('csv_data', {})
    target_file = csv_params.get('file_name', '')
    with open(target_file, 'w') as csv_file:
        file_writer = csv.writer(
            csv_file,
            delimiter=',',
            quoting=csv.QUOTE_MINIMAL,
        )
        file_writer.writerow(header)
        for key in commits.keys():
            for row in commits[key]:
                row = [
                    key,
                    row['committer_name'],
                    row['committed_date'],
                    row['title'],
                ]  # to ensure correct order with header, can be replaced with `row = [key] + [row]`
                file_writer.writerow(row)


CSV_DATA = get_project_commits_data(file_name='project_details.csv')
HEADER = [
    'Project name',
    'Developer',
    'Commit date',
    'Commit message',
]
send_commits_to_file(
    header=HEADER,
    csv_data=CSV_DATA,
    file_name=TARGET_FILE_PATH,
)
