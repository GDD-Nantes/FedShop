import sys
from urllib.parse import urlparse

# Define a filename.
filename = sys.argv[1]

#print(filename)

# Open the file as f.
# The function readlines() reads the file.
with open(filename) as f:
    content = f.readlines()

# Show the file contents line by line.
# We added the comma to print single newlines and not double newlines.
# This is because the lines contain the newline character '\n'.
for line in content:
    #print(f"{line}")
    triple=line.split("\t")
    prov=f"http://{urlparse(triple[0][1:]).netloc}"
    print(f"{triple[0]}\t{triple[1]}\t{triple[2][:-2]}\t<{prov}> .")
