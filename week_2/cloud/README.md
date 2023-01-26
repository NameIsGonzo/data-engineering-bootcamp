# Second week

## Requirements
****
- GCP Service account with the following roles:
  - Viewer
  - BigQuery Admin
  - Storage Admin
  - Storage Object Admin

- JSON credential key for previous service account
- Prefect GCP plugin

## Introduction
****
On this week we are going to create two python files.

The first will be an ETL pipeline that allows the user to use parameters instead of hard-coding the values.

The second python file will be our docker deployment, so we can containerize our pipeline.

## Steps
****

### 1. Dockerhub 
First we will need to log in into our docker account. This can be easily done with the following command
```commandline
docker login -u <user-name> -p <user-password> docker.io
```
**IMPORTANT NOTE !!**

**-------------------------------------------------------------------------------------------------------------------**

THIS IS NOT THE BEST WAY TO DEPLOY OUR APPLICATION BUT FOR THE SAKE OF THE EXERCISE WE WILL DO IT
IN THIS WAY, JUST MAKE SURE TO DON'T PUSH YOUR CREDENTIALS INTO GITHUB AND MAKE YOUR DOCKERHUB REPO PRIVATE

**-------------------------------------------------------------------------------------------------------------------**

Once you are logged in your account, make a new directory inside week_2/ directory and name it
**nothing_to_see_here**, after that, place your service account credentials inside the previous directory.

Now that we have our creds in the desired dir, we will need to build the dockerfile image with the following command
```commandline
docker image build -f cloud/Dockerfile -t <user-name>/<repository-name>:<tag> .
```

The **tag** can be anything, for better versioning use v1, v2 and so on.

Now let's push our docker image into dockerhub.
```commandline
docker image push <user-name>/<repository-name>:<tag>
```

## 2. Prefect Orion UI
****

Let's load our docker container into a block. Go to the following url
```
http://127.0.0.1:4200/
```

1. Once you are in the UI go to the **Blocks** section and add a new block.
2. Search **Docker Container** click Add +
3. Block name should be = **prefect-zoom**
4. Image use the one you use while creating the docker image: <user-name>/<repository-name>:<tag>
5. Set **Auto Remove** to True, otherwise everytime a run success the containers will not be destroyed.
6. Click Create

## 3. Execution
****

Once the Docker image it's set and the Docker Container block it's added you should be able to run the pipeline
without any problem using the following command:

```commandline
prefect deployment run etl-parent-flow/docker-flow --params='{"color": "yellow", "year": 2022, "months": [4,5]}
```

You can always change the parameters of the pipeline here are the options you can use:

**IMPORTANT**

1. The Green taxi data it's only available from 2014 
2. For-Hire Vehicle data (fhv) it's only available from 2015
3. High Volume For-Hire data it's only available from February 2019

Color: [yellow, green, fhv, fhvhv]

Year: [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]

Months: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]



