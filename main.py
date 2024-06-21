import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Tuple, List

import createrepo_c


def get_repodata_by_repo_url(repo_url: str) -> Dict:
    repodata = createrepo_c.Metadata()
    repodata.locate_and_load_xml(repo_url)
    return {
        key: {'name': repodata.get(key).name, 'epoch': repodata.get(key).epoch, 'version': repodata.get(key).version,
              'release': repodata.get(key).release, 'arch': repodata.get(key).arch,
              'nevra': repodata.get(key).nevra(), }
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


def parse_merged_repos(merged_packages: Dict) -> Tuple[Dict, Dict, Dict]:
    name_dict = {}
    nevra_dict = {}
    package_data_dict = {}

    for package in merged_packages.values():
        name = package['name']
        epoch = package['epoch']
        version = package['version']
        release = package['release']
        arch = package['arch']
        nevra = package['nevra']
        name_dict[name] = ''
        nevra_dict[nevra] = {'name': name, 'epoch': epoch, 'version': version, 'release': release, 'arch': arch}
        name_arch = name + '.' + arch
        if name_arch in package_data_dict:
            if package_data_dict[name_arch]['epoch'] > epoch:
                continue
            if package_data_dict[name_arch]['version'] > version:
                continue
            if package_data_dict[name_arch]['release'] > release:
                continue
        package_data_dict[name_arch] = {'epoch': epoch, 'version': version, 'release': release}
    return name_dict, package_data_dict, nevra_dict


def get_unique_packages_by_name(first_dict: Dict, second_dict: Dict) -> Tuple[List, List]:
    unique_first = []
    keys = first_dict.copy().keys()
    for key in keys:
        if key in second_dict:
            first_dict.pop(key)
            second_dict.pop(key)
        else:
            unique_first.append(key)

    return unique_first, list(second_dict.keys())


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


def get_unique_packages_by_nevra(first_dict: Dict, second_dict: Dict) -> Tuple[Dict, Dict]:
    for key in first_dict.copy().keys():
        if key in second_dict:
            first_dict.pop(key)
            second_dict.pop(key)
    return first_dict, second_dict


def get_youngest_namesake_packages(first_dict: Dict, second_dict: Dict):
    out = []
    for key in first_dict.keys():
        # print(key, first_dict.get(key), second_dict.get(key, 'none'))
        if key in second_dict:
            first = first_dict[key]['epoch'] + ':' + first_dict[key]['version'] + '.' + first_dict[key]['release']
            second = second_dict[key]['epoch'] + ':' + second_dict[key]['version'] + '.' + second_dict[key]['release']
            out.append({key: [first, second]})
    return out


def save_to_json(file_name: str, obj) -> None:
    with open(file_name, 'w') as out:
        json.dump(obj, out)


if __name__ == '__main__':
    start = time.time()

    alpha_links = ['http://repo.red-soft.ru/redos/7.3/x86_64/os/', 'http://repo.red-soft.ru/redos/7.3/x86_64/updates/']
    # beta_links = ['http://repo.red-soft.ru/redos/7.3c/x86_64/os/', 'http://repo.red-soft.ru/redos/7.3c/x86_64/updates/']
    # beta_links = ['http://repo.red-soft.ru/redos/8.0/x86_64/os/', 'http://repo.red-soft.ru/redos/8.0/x86_64/updates/']
    beta_links = ['http://mirror.centos.org/centos/7/os/x86_64', 'http://mirror.centos.org/centos/7/updates/x86_64']

    with ProcessPoolExecutor() as executor:
        future_alpha = executor.submit(merge_repo, alpha_links)
        future_beta = executor.submit(merge_repo, beta_links)
        alpha = future_alpha.result()
        beta = future_beta.result()
    end = time.time()
    alpha_name_dict, alpha_package_data_dict, alpha_nerva_dict = parse_merged_repos(alpha)
    beta_name_dict, beta_package_data_dict, beta_nerva_dict = parse_merged_repos(beta)
    alpha_unique_by_name, beta_unique_by_name = get_unique_packages_by_name(alpha_name_dict, beta_name_dict)
    version_differed, release_differed, version_release_differed = get_differed_by_version_release(
        alpha_package_data_dict, beta_package_data_dict)

    alpha_unique_by_nerva, beta_unique_by_nerva = get_unique_packages_by_nevra(alpha_nerva_dict, beta_nerva_dict)
    youngest_namesake_packages = get_youngest_namesake_packages(alpha_package_data_dict, beta_package_data_dict)
    end2 = time.time()

    save_to_json('alpha_unique_by_name.json', alpha_unique_by_name)
    save_to_json('beta_unique_by_name.json', beta_unique_by_name)
    save_to_json('version.json', version_differed)
    save_to_json('release.json', release_differed)
    save_to_json('version_and_release.json', version_release_differed)
    save_to_json('alpha_unique_by_nerva.json', alpha_unique_by_nerva)
    save_to_json('beta_unique_by_nerva.json', beta_unique_by_nerva)
    save_to_json('youngest_namesake_packages.json', youngest_namesake_packages)

    print(f"Execution time: {end - start} seconds")
    print(f"Execution time: {end2 - end} seconds")
