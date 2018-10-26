# -*- coding: utf-8 -*-
import csv
import json
import sys
sys.setdefaultencoding('utf-8')

csvfile = open('googleplaystore.csv', 'r')
jsonfile = open('googleplaystore.json', 'w')

fieldnames = (
"App", "Category", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content Rating", "Genres", "Last Updated",
"Current Ver", "Android Ver")
reader = csv.DictReader(csvfile,fieldnames)
for row in reader:
    json.dump(row,jsonfile)
    jsonfile.write('\n')
