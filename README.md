# Google Cloud Compute Engine Automation
Automate main.py with Google Cloud Compute Engine


[Google Cloud Compute Engine Docs](https://cloud.google.com/compute)

-Setup a vitrual machine instance.


-Create Directory for project files and save the project files to the directory.


-Install python and all the module in the requirements.txt file


-Setup a crontab job `crontab -e`, use this command to run the script at 5am everyday `0 5 * * * * python3 (path of main.py) 2>&1 > (path to save a crontab log file)`
