import subprocess
import sys
import pandas as pd

scripts = [
	"json_convertor.py",
	"json_participant.py",
	"json_engagement.py",
	"explore-exploit.py"
	]

for script in scripts:
	subprocess.run([sys.executable, script], check=False)
	
	


