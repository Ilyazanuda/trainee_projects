import os
import argparse
import re
import sys
import configparser


def get_args():
    parser = argparse.ArgumentParser(prog='Setup_dataset')
    parser.add_argument('--new_data',
                        action='store_true')
    args = parser.parse_args()

    return {'new_data': args.new_data}


def get_cfg(cfg_path='config/config.ini'):
    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)
    cfg_dict = {}

    for section in cfg.sections():
        cfg_dict.setdefault(section, {})
        cfg_dict[section].update({param: cfg[section][param] for param in cfg[section]})

    return cfg_dict


def new_download_and_put(cfg):
    os.system(f'hdfs dfs -rm -r {cfg["PATHS"]["data_path"]}')
    os.system(f'hdfs dfs -mkdir -p {cfg["PATHS"]["data_path"]}')
    os.system(f'rm -r movies_data')
    os.system(f'wget {cfg["PATHS"]["link_data"]}')
    os.system(f'unzip {cfg["PATHS"]["archive_name"]}*')
    os.system(f'rm -r {cfg["PATHS"]["archive_name"]}.*')
    os.system(f'mv {cfg["PATHS"]["archive_name"]}/ movies_data/')
    os.system(f'hdfs dfs -put movies_data/m* {cfg["PATHS"]["data_path"]}/')
    os.system(f'hdfs dfs -put movies_data/r* {cfg["PATHS"]["data_path"]}/')


def put_by_local(cfg):
    os.system(f'hdfs dfs -rm -r {cfg["PATHS"]["data_path"]}')
    os.system(f'hdfs dfs -mkdir -p {cfg["PATHS"]["data_path"]}')
    os.system(f'hdfs dfs -put movies_data/m* {cfg["PATHS"]["data_path"]}/')
    os.system(f'hdfs dfs -put movies_data/r* {cfg["PATHS"]["data_path"]}/')


def check_folders(cfg, args):
    try:
        if not os.system(f'hdfs dfs -ls {cfg["PATHS"]["prog_path"]}'):
            if not os.system(f'hdfs dfs -ls {cfg["PATHS"]["result_path"]}'):
                os.system(f'hdfs dfs -rm -r {cfg["PATHS"]["result_path"]}')
        else:
            os.system(f'hdfs dfs -mkdir -p {cfg["PATHS"]["prog_path"]}')

        if not os.system(f'hdfs dfs -ls {cfg["PATHS"]["data_path"]}') \
                and not os.system(f'hdfs dfs -ls {cfg["PATHS"]["data_path"]}/m*.csv') \
                and not os.system(f'hdfs dfs -ls {cfg["PATHS"]["data_path"]}/r*.csv'):
            # hdfs has data folder and files in it
            if args['new_data']:
                # received arg new_data
                new_download_and_put(cfg)
        else:
            # hdfs has not data folder
            if os.system(f'ls -l movies_data') \
                    or os.system(f'ls -l movies_data/m*.csv') \
                    or os.system(f'ls -l movies_data/r*.csv'):
                # local folder movies_data not exists or not exists local folder movies_data
                new_download_and_put(cfg)
            else:
                # data movies files exists local folder movies_data
                put_by_local(cfg)
    except Exception as err:
        print(f'Received: {err}', file=sys.stderr)


def main():
    args = get_args()
    cfg = get_cfg()

    cfg['PATHS']['archive_name'] = re.findall(r'([^/]+)(?=\.)', cfg['PATHS']['link_data'])[-1]
    cfg['PATHS']['data_path'] = cfg['PATHS']['prog_path'] + '/data'

    check_folders(cfg, args)


if __name__ == '__main__':
    main()
