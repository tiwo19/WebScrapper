from flask import Flask, jsonify
import subprocess
import threading
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables

application = Flask(__name__)

@application.route('/run', methods=['POST'])
def run_script():
    """Endpoint to trigger the long-running script"""
    def execute_script():
        try:
            with open("/tmp/index.log", "a") as logfile:
                process = subprocess.Popen(
                    ['python3', '-u', 'index.py'],
                    stdout=logfile,
                    stderr=logfile,
                    text=True
                )
                process.wait()
                if process.returncode != 0:
                    logfile.write(f"index.py exited with code {process.returncode}\n")
        except Exception as e:
            with open("/tmp/index.log", "a") as logfile:
                logfile.write(f"Script crashed: {e}\n")

    thread = threading.Thread(target=execute_script)
    thread.daemon = True
    thread.start()

    return jsonify({
        "status": "success",
        "message": "Script started in background",
        "pid": os.getpid()
    })

@application.route('/health')
def health_check():
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=8000)
