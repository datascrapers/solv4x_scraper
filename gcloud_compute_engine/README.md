# Google Cloud Compute Engine Automation
Automate `main.py` with Google Cloud Compute Engine

[Google Cloud Compute Engine Documentation](https://cloud.google.com/compute)

## Setup Instructions
- Setup a virtual machine instance.

- Create a directory for your project and save the project files to the directory.

- Install python and all the modules in the `requirements.txt` file.

- Setup a crontab job with
  ```sh
  crontab -e
  ```
  then input the following command to run the script at 5am everyday
  ```
  0 5 * * * * python3 <path of main.py> 2>&1 > [path to save a crontab log file]
  ```
