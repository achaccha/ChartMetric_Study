import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--chart', nargs='*', default=['viral', 'regional'])
parser.add_argument('--duration', nargs='*', default=['weekly', 'daily'])
parser.add_argument('--country', nargs='+')
parser.add_argument('--date', nargs='+')
parser.add_argument('--dbaction', choices=['insert','update','delete','query'] )

args = parser.parse_args()
print(args.chart)
print(type(args.chart))

print(args.dbaction)


