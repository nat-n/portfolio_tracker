from os import environ, path
import json

env = environ.get('ENV', 'dev')
config_path = path.abspath(
    path.realpath(__file__ + '/../../configuration/' + env + '.json'))
raw_config = json.load(open(config_path))


def resolve_config(root, path=''):
    if isinstance(root, str) and root[0] == '$':
        value = environ.get(root[1:])
        if value is None:
            print(
                'Warning: no environmental variable set for ' +
                path + ' : ' + root)
        return [[path, value]]
    if isinstance(root, dict):
        return [
            item for sublist in [
                resolve_config(
                    root[key],
                    ('' if path == '' else path + '.') + key
                )
                for key in root.keys()
            ] for item in sublist
        ]
    return [[path, root]]


config = dict(resolve_config(raw_config))
