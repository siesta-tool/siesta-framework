import subprocess
import sys

if __name__ == "__main__":
    print("Starting Siesta API and Streamlit UI...")
    
    # Start the core API server
    api_process = subprocess.Popen([sys.executable, "siesta/main.py"])
    
    # Start the Streamlit Web UI
    ui_process = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "ui/main.py"])
    
    try:
        api_process.wait()
        ui_process.wait()
    except KeyboardInterrupt:
        print("\nShutting down Siesta framework...")
        api_process.terminate()
        ui_process.terminate()
        api_process.wait()
        ui_process.wait()