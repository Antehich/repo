import xml.etree.ElementTree as ET

import requests


def get_primary_checksum(url):
    url = url + "repodata/repomd.xml"
    xml_content = requests.get(url).content
    root = ET.fromstring(xml_content)
    namespaces = {
        'repo': 'http://linux.duke.edu/metadata/repo',
        'rpm': 'http://linux.duke.edu/metadata/rpm'
    }
    primary_data = root.find("repo:data[@type='primary']", namespaces)

    if primary_data is not None:
        checksum = primary_data.find('repo:checksum', namespaces).text
        return checksum
    else:
        return None
