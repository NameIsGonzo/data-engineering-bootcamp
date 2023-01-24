# Second week

## Requirements
****
Run in your terminal the next command to install all the dependencies that we will need. 
(You'll need to be in your venv first.)

```commandline
pip install -r requirements.txt
```

## Execute Orion UI
****
Execute the following command in your terminal
```commandline
prefect orion start
```
Now open a new tab in your favorite web browser and go to **http://127.0.0.1:4200** 

## Execute our ETL pipeline
Go to the following directory
```
de_bootcamp/:
	|
	|------ README.md
	|------ week_1/
	|------ week_2/ # Here****
	          |----- local/
	          |----- cloud/
	          |----- dataset/
```
and execute the python file data_ingestion_flow.py
```commandline
python local/data_ingestion_flow.py
```
You will start to see our flow and task being executed one after one.
Additionally you can see the progress of our pipeline if you go to our Orion UI.
