#!/bin/bash

base_dir="output"

for dir in "$base_dir"/*; do
  if [ -d "$dir" ]; then
    name=$(basename "$dir")
    file1="$dir/${name}.txt"
    file2="$dir/${name}_optimzed.txt"

    if [[ -f "$file1" && -f "$file2" ]]; then
      if diff -q "$file1" "$file2" > /dev/null; then
        echo "[✓] $name: Files are the same."
      else
        echo "[✗] $name: Files are different."
      fi
    else
      echo "[!] $name: Missing one or both files."
    fi
  fi
done
