import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Tuple

import createrepo_c


def get_repodata_by_repo_url(repo_url: str) -> Dict:
    repodata = createrepo_c.Metadata()
    repodata.locate_and_load_xml(repo_url)
    return {key: {'name': repodata.get(key).name, 'version': repodata.get(key).version,
                  'release': repodata.get(key).release}
            for key in repodata.keys()}


def merge_repo(links) -> Dict:
    with ProcessPoolExecutor() as executor:
        future_to_url = {executor.submit(get_repodata_by_repo_url, link): link for link in links}
        accumulator = {}
        for future in as_completed(future_to_url):
            link = future_to_url[future]
            try:
                data = future.result()
                accumulator.update(data)
                print(f"Completed fetching data from {link}")
            except Exception as exc:
                print(f"Error fetching data from {link}: {exc}")
    return accumulator


def parse_merged_repos(merged_packages: Dict) -> Tuple[Dict, Dict]:
    name_dict = {}
    version_release_dict = {}

    for pkg_data in merged_packages.values():
        name = pkg_data['name']
        version = pkg_data['version']
        release = pkg_data['release']
        name_dict[name] = {'version': version, 'release': release}
        version_release_dict[name] = pkg_data

    return name_dict, version_release_dict


def get_unique_packages(first_dict: Dict, second_dict: Dict) -> Tuple[Dict, Dict]:
    unique_first = {k: v for k, v in first_dict.items() if k not in second_dict}
    unique_second = {k: v for k, v in second_dict.items() if k not in first_dict}
    return unique_first, unique_second


def get_differed_by_version_release(first_dict: Dict, second_dict: Dict) -> Tuple[Dict, Dict, Dict]:
    version_dict = {}
    release_dict = {}
    version_release_dict = {}

    for key in first_dict.keys():
        a = first_dict[key]
        b = second_dict.get(key)
        if not b:
            continue
        if a['version'] == b['version'] and a['release'] != b['release']:
            release_dict[key] = (a['release'], b['release'])
        elif a['version'] != b['version'] and a['release'] == b['release']:
            version_dict[key] = (a['version'], b['version'])
        elif a['version'] != b['version'] and a['release'] != b['release']:
            version_release_dict[key] = (a['version'] + a['release'], b['version'] + b['release'])
    return version_dict, release_dict, version_release_dict


def save_to_json(file_name: str, obj) -> None:
    with open(file_name, 'w') as out:
        json.dump(obj, out)


if __name__ == '__main__':
    start = time.time()

    alpha_links = ['http://repo.red-soft.ru/redos/7.3/x86_64/os/', 'http://repo.red-soft.ru/redos/7.3/x86_64/updates/',
                   'http://repo.red-soft.ru/redos/7.3/x86_64/kernel-testing/']
    beta_links = ['http://repo.red-soft.ru/redos/8.0/x86_64/os/', 'http://repo.red-soft.ru/redos/8.0/x86_64/updates/',
                  'http://repo.red-soft.ru/redos/8.0/x86_64/kernel-testing/']

    with ProcessPoolExecutor() as executor:
        future_alpha = executor.submit(merge_repo, alpha_links)
        future_beta = executor.submit(merge_repo, beta_links)
        alpha = future_alpha.result()
        beta = future_beta.result()
    end = time.time()
    alpha_name_dict, alpha_version_release_dict = parse_merged_repos(alpha)
    beta_name_dict, beta_version_release_dict = parse_merged_repos(beta)

    alpha_unique, beta_unique = get_unique_packages(alpha_name_dict, beta_name_dict)
    version_differed, release_differed, version_release_differed = get_differed_by_version_release(
        alpha_version_release_dict, beta_version_release_dict)

    save_to_json('alpha_unique.json', alpha_unique)
    save_to_json('beta_unique.json', beta_unique)
    save_to_json('version.json', version_differed)
    save_to_json('release.json', release_differed)
    save_to_json('version_and_release.json', version_release_differed)
    end2 = time.time()

    print(f"Execution time: {end - start} seconds")
    print(f"Execution time: {end2 - end} seconds")
