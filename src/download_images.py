import requests
import boto3
from time import sleep
from datetime import datetime
import os, errno
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


s3 = boto3.client('s3')



def write_image(local_name, content):
    local_name = local_name.replace('cctv/', '')
    s3.put_object(Bucket='indy-cctv', Body=content, Key=local_name)



def main():

    base_url = 'http://pws.trafficwise.org'
    cctv = requests.get(f'{base_url}/aries/cctv.json')
    cc_json = cctv.json()

    stamp = f'{datetime.now().strftime("%Y_%m_%d_%H_%M")}'

    for f in cc_json['features']:
        if f['properties']['assettype'] == 'cctv':
            img = f['properties']['image'].split('/')[2]
            name_parts = img.split('.')
            local_name = f'cctv/{name_parts[0]}/{stamp}.{name_parts[1]}'
            logging.info(f"LocalName: {local_name}")
            sleep(0.2)
            resp = requests.get(f'{base_url}/{f["properties"]["image"]}', allow_redirects=True)
            if resp.status_code != 200:
                continue
            write_image(local_name, resp.content)


if __name__ == "__main__":
    main()