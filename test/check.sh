python check.py ./data ./data ./out ./res
# cat ./out|ruby -ne 'a=$_.split; puts $_ if a[0].to_i == 1000'
# cat ./out|sort -n -k1

