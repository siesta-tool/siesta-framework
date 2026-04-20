#!/usr/bin/env python3

"""
Script to install all dependencies from requirements.txt files in the project.
"""

import os
import subprocess
import sys

def install_dependencies():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    requirements_files = []

    # Project root requirements
    root_req = os.path.join(project_root, "requirements.txt")
    if os.path.exists(root_req) and root_req not in requirements_files:
        requirements_files.append(root_req)

    # Subdirectories of siesta
    target_dirs = ["tests"]
    for d in target_dirs:
        target_path = os.path.join(project_root, d)
        if os.path.exists(target_path):
            for root_dir, dirs, files in os.walk(target_path):
                # Skip hidden and environment directories
                dirs[:] = [d_name for d_name in dirs if not d_name.startswith('.') and d_name not in ('venv', 'env', '__pycache__')]
                if "requirements.txt" in files:
                    req_path = os.path.join(root_dir, "requirements.txt")
                    if req_path not in requirements_files:
                        requirements_files.append(req_path)

    
    print(f"Found {len(requirements_files)} requirement files.")
    
    # 3. Install them
    for req_file in requirements_files:
        print(f"Installing from {req_file}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_file])

if __name__ == "__main__":
    install_dependencies()