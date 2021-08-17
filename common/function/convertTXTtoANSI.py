import io
import os
import glob
import yaml
def convertTXTToANSI(path):
  if not os.path.exists(path):
      os.makedirs(path)
  else:
      os.chdir(path)

  files = glob.glob("*-t.txt")

  dfs = []

  for filename in files:
      with io.open(filename, mode="r", encoding="utf16") as fd:
          content = fd.read()
      with io.open(filename[:(len(filename)-6)] + '.txt', mode="w", encoding="cp1252") as fd:
          fd.write(content)