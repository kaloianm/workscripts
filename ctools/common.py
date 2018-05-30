# Helper utilities used by the ctools scripts
#

import sys


# Function for a Yes/No result based on the answer provided as an argument
def yes_no(answer):
    yes = set(['yes', 'y', 'ye', ''])
    no = set(['no', 'n'])

    while True:
        choice = input(answer).lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print("Please respond with 'yes' or 'no'\n")


# Abstracts constructing the name of an executable on POSIX vs Windows platforms
def exe_name(name):
    if (sys.platform == 'win32'):
        return name + '.exe'
    return name
