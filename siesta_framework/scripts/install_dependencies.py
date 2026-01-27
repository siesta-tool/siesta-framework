#!/usr/bin/env python3

"""
Script to install all dependencies from requirements.txt files in the project.
"""

import os
import subprocess
import sys

def install_dependencies():
    root = os.getcwd()
    requirements_files = []
    
    # 1. Find root requirements
    if os.path.exists("requirements.txt"):
        requirements_files.append("requirements.txt")

    # 2. Find all requirements
    dirs = ["core", "modules", "storage", "api", "scripts"]
    for dir in dirs:
        modele_dir = os.path.join(root, dir)
        if os.path.exists(modele_dir):
            for root_dir, dirs, files in os.walk(modele_dir):
                for file in files:
                    if file == "requirements.txt":
                        requirements_files.append(os.path.join(root_dir, file))

    
    print(f"Found {len(requirements_files)} requirement files.")
    
    # 3. Install them
    for req_file in requirements_files:
        print(f"Installing from {req_file}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_file])

if __name__ == "__main__":
    install_dependencies()