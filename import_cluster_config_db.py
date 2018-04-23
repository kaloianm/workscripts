#! python
#

import argparse

def main():
    argsParser = argparse.ArgumentParser(description='Tool to interpet a cluster config database and construct a cluster with exactly the same configuration')
    argsParser.add_argument('--binarypath', help='Directory containing the MongoDB binaries', required=True)
    argsParser.add_argument('--datapath', help='Directory where to place the data files (will create subdirectories)', required=True)

    args = argsParser.parse_args()
    print args

if __name__ == "__main__":
    main()
