#!/usr/bin/python3
# Usage: ./bin/oozie-lineage.py ./oozie --search webrequest
import os
import xml.etree.ElementTree as ET
import argparse


def main(oozie_path, search=None):
    coords = [
        os.path.join(root, 'coordinator.xml')
        for (root, dirs, files) in os.walk(oozie_path)
        if 'coordinator.xml' in files
    ]

    parsedCoords = [(name, ET.parse(name)) for name in coords]

    details = [
        {
            'coord': name,
            'inputs': [x.attrib['dataset'] for x in coord.findall('.//{uri:oozie:coordinator:0.4}data-in')],
            'outputs': [x.attrib['dataset'] for x in coord.findall('.//{uri:oozie:coordinator:0.4}data-out')]
        }
        for (name, coord) in parsedCoords
    ]

    if search:
        def recurse(searching, indent):
            matches = [
                (d['coord'], d['outputs'])
                for d in details
                if any([searching in d_in for d_in in d['inputs']])
            ]
            for m in matches:
                trimmed = m[0][2:-16]
                print(' ' * indent + trimmed)
                if 'webrequest/load' in trimmed:
                    # Webrequest load has the same input and output names and will just recurse forever
                    continue
                for output in m[1]:
                    recurse(output, indent + 4)

        recurse(search, 0)

    else:
        for d in details:
            print(d['coord'])
            print('     IN: ' + ', '.join(d['inputs']))
            print('    OUT: ' + ', '.join(d['outputs']))
            print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''For every oozie coordinator, list inputs and outputs.  Can search.
                       Example: ./bin/oozie-lineage.py ./oozie --search webrequest''')
    parser.add_argument('oozie_path', type=str, help='oozie jobs folder')
    parser.add_argument('--search', type=str, help='find all lineage descending from datasets that match this string')

    known_args, other_args = parser.parse_known_args()
    main(known_args.oozie_path, known_args.search)
